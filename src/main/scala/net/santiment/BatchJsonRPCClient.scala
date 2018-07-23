package net.santiment

import java.io.{ByteArrayOutputStream, IOException, InputStream, OutputStream}
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader

import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode, ObjectMapper}
import com.googlecode.jsonrpc4j.{JsonRpcHttpClient, ReadContext}
import javax.net.ssl.{HostnameVerifier, HttpsURLConnection, SSLContext}
import java.net.{HttpURLConnection, Proxy, URL}
import java.util.zip.GZIPOutputStream
import java.io.IOException
import java.util.zip.GZIPInputStream

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.googlecode.jsonrpc4j.DefaultExceptionResolver
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.reflect.ClassTag

object BatchJsonRPCClient {
  val ACCEPT_ENCODING = "Accept-Encoding"
  val GZIP = "gzip"
  val CONNECTION_TIMEOUT_MILLIS: Int = 60 * 1000
  val READ_TIMEOUT_MILLIS: Int = 60*1000*2
  val CONTENT_ENCODING = "Content-Encoding"
  val JSONRPC_CONTENT_TYPE = "application/json-rpc"

  val JSONRPC = "jsonrpc"
  val VERSION = "2.0"
  val METHOD = "method"
  val PARAMS = "params"
  val ERROR = "error"
  val RESULT = "result"

  val exceptionResolver: DefaultExceptionResolver = DefaultExceptionResolver.INSTANCE

}

class BatchJsonRPCClient(val mapper:ObjectMapper = new ObjectMapper(),
                         val serviceUrl:URL,
                         var headers: Map[String,String] = Map[String,String](),
                         val gzipRequests: Boolean = false,
                         val acceptGzipResponses: Boolean = false,
                         val hostNameVerifier: HostnameVerifier = null,
                         val sslContext: SSLContext = null,
                         val contentType:String = BatchJsonRPCClient.JSONRPC_CONTENT_TYPE
                        )
extends LazyLogging {

  import BatchJsonRPCClient._
  val connectionProxy: Proxy = Proxy.NO_PROXY

  if (acceptGzipResponses) {
    headers = headers.updated(ACCEPT_ENCODING, GZIP)
  }

  /**
    * Prepares a connection to the server.
    *
    * @param extraHeaders extra headers to add to the request
    * @return the unopened connection
    * @throws IOException
    */
  @throws[IOException]
  def prepareConnection(extraHeaders: Map[String, String]): HttpURLConnection = { // create URLConnection
    val connection = serviceUrl.openConnection(connectionProxy).asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(CONNECTION_TIMEOUT_MILLIS)
    connection.setReadTimeout(READ_TIMEOUT_MILLIS)
    connection.setAllowUserInteraction(false)
    connection.setDefaultUseCaches(false)
    connection.setDoInput(true)
    connection.setDoOutput(true)
    connection.setUseCaches(false)
    connection.setInstanceFollowRedirects(true)
    connection.setRequestMethod("POST")
    setupSsl(connection)
    addHeaders(extraHeaders, connection)
    connection
  }

  private def setupSsl(connection: HttpURLConnection): Unit = {
    if (classOf[HttpsURLConnection].isInstance(connection)) {
      val https = classOf[HttpsURLConnection].cast(connection)
      if (hostNameVerifier != null) https.setHostnameVerifier(hostNameVerifier)
      if (sslContext != null) https.setSSLSocketFactory(sslContext.getSocketFactory)
    }
  }

  private def addHeaders(extraHeaders: Map[String,String], connection: HttpURLConnection): Unit = {
    connection.setRequestProperty("Content-Type", contentType)
    for (entry <- headers) {
      connection.setRequestProperty(entry._1, entry._2)
    }

    for (entry <- extraHeaders) {
      connection.setRequestProperty(entry._1, entry._2)
    }
  }

  private def useGzip(connection: HttpURLConnection) : Boolean = {
    val contentEncoding = connection.getHeaderField(CONTENT_ENCODING)
    contentEncoding != null && contentEncoding.equalsIgnoreCase(GZIP)
  }


  @throws[IOException]
  private def getStream(inputStream: InputStream, useGzip: Boolean) : InputStream = {
    if (useGzip) new GZIPInputStream(inputStream)
    else inputStream
  }

  def writeRequest(methodName:String, arguments:Array[Array[Object]], out:OutputStream ): Unit ={

    val request = new ArrayNode(mapper.getNodeFactory)

    for (args <- arguments) yield {
      var obj = mapper.createObjectNode()
      obj.put(JSONRPC, VERSION)
      obj.put(METHOD, methodName)
      val params: ArrayNode = new ArrayNode(mapper.getNodeFactory)
      for (arg <- args) {
        val argNode:JsonNode = mapper.valueToTree(arg)
        params.add(argNode)
      }

      obj.set(PARAMS, params)

      request.add(obj)
    }

    mapper.writeValue(out, request)
    out.flush()
  }

  protected def hasError(jsonObject: ObjectNode): Boolean = {
    jsonObject.has(ERROR) && jsonObject.get(ERROR) != null && !jsonObject.get(ERROR).isNull
  }

  @throws[Throwable]
  protected def handleErrorResponse(jsonObject: ObjectNode): Unit = {
    if (hasError(jsonObject)) { // resolve and throw the exception
      if (exceptionResolver == null) throw DefaultExceptionResolver.INSTANCE.resolveException(jsonObject)
      else throw exceptionResolver.resolveException(jsonObject)
    }
  }

  private def hasResult(jsonObject: ObjectNode):Boolean = {
    jsonObject.has(RESULT) && !jsonObject.get(RESULT).isNull
  }

  def readResponse[T : ClassTag ](input:InputStream) : Array[T] = {

    val responses:Array[JsonNode] = mapper.readValue(input,classOf[Array[JsonNode]])

    val tag = implicitly[ClassTag[T]]

    val result:Array[T] = for (response:JsonNode <- responses) yield {
      val jsonObject = response.asInstanceOf[ObjectNode]
      handleErrorResponse(jsonObject)

      if (hasResult(jsonObject)) {
        val returnJsonParser = mapper.treeAsTokens(jsonObject.get(RESULT))
        mapper.readValue(returnJsonParser, tag.runtimeClass).asInstanceOf[T]
      } else {
        null.asInstanceOf[T]
      }
    }

    result
  }


  private def readErrorString(connection: HttpURLConnection): String = {
    val stream = connection.getErrorStream
    try {
      val buffer = new StringBuilder
      val reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"))

      try {
        var ch = reader.read
        while ( {
          ch >= 0
        }) {
          buffer.append(ch.toChar)

          ch = reader.read
        }
      } finally if (reader != null) reader.close()

      buffer.toString
    } catch {
      case e: IOException =>
        e.getMessage
    } finally if (stream != null) stream.close()
  }


  /**
    * {@inheritDoc }
    */
  @throws[Throwable]
  def invoke[T : ClassTag](methodName: String, arguments: Array[Array[Object]], extraHeaders: Map[String,String]): Array[T] = {
    val connection = prepareConnection(extraHeaders)
    logger.debug("Connection prepared")
    try {
      if (this.gzipRequests) {
        connection.setRequestProperty(CONTENT_ENCODING, GZIP)
        val baos = new ByteArrayOutputStream
        val gos = new GZIPOutputStream(baos)
        try {
          logger.debug("writing gzip")
          writeRequest(methodName, arguments, gos)
          gos.flush()
        }
        finally { if (gos != null) gos.close() }

        connection.setFixedLengthStreamingMode(baos.size)
        logger.debug("Connecting")
        connection.connect()
        logger.debug("sending gzip")
        connection.getOutputStream.write(baos.toByteArray)
      }
      else {
        logger.debug("connecting")
        connection.connect()
        logger.debug("sending")
        val send = connection.getOutputStream
        try {
          writeRequest(methodName, arguments, send)
          send.flush()
        }
        finally { if (send != null) send.close() }
      }

      // read and return value
      try {
        logger.debug("start reading")
        val answer = getStream(connection.getInputStream, useGzip(connection))
        try {
          logger.debug("reading")
          readResponse(answer)
        }
        finally if (answer != null) answer.close()
      }
      catch {
        case e: JsonMappingException =>
          // JsonMappingException inherits from IOException
          throw e
        case e: IOException =>
          if (connection.getErrorStream == null) throw new RuntimeException("Caught error with no response body.", e)
          val answer = getStream(connection.getErrorStream, useGzip(connection))
          try
            readResponse(answer)
          catch {
            case ef: IOException =>
              throw new RuntimeException(readErrorString(connection), ef)
          } finally if (answer != null) answer.close()
      }
    } finally connection.disconnect()
  }
}

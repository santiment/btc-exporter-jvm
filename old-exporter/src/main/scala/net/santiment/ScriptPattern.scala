package net.santiment

import org.bitcoinj.script.{Script, ScriptOpCodes}

object ScriptPattern {

  import org.bitcoinj.script.ScriptOpCodes._
  import com.google.common.base.Preconditions.checkArgument

  val WITNESS_PROGRAM_LENGTH_PKH = 20
  val WITNESS_PROGRAM_LENGTH_SH = 32
  val WITNESS_PROGRAM_MIN_LENGTH = 2
  val WITNESS_PROGRAM_MAX_LENGTH = 40
  val LEGACY_ADDRESS_LENGTH = 20

  /**
    * Returns true if this script is of the form {@code DUP HASH160 <pubkey hash> EQUALVERIFY CHECKSIG}, ie, payment to an
    * address like {@code 1VayNert3x1KzbpzMGt2qdqrAThiRovi8}. This form was originally intended for the case where you wish
    * to send somebody money with a written code because their node is offline, but over time has become the standard
    * way to make payments due to the short and recognizable base58 form addresses come in.
    */
  def isPayToPubKeyHash(script: Script): Boolean = {
    val chunks = script.getChunks
    if (chunks.size != 5) return false
    if (!chunks.get(0).equalsOpCode(OP_DUP)) return false
    if (!chunks.get(1).equalsOpCode(OP_HASH160)) return false
    val chunk2data = chunks.get(2).data
    if (chunk2data == null) return false
    if (chunk2data.length != LEGACY_ADDRESS_LENGTH) return false
    if (!chunks.get(3).equalsOpCode(OP_EQUALVERIFY)) return false
    if (!chunks.get(4).equalsOpCode(OP_CHECKSIG)) return false
    true
  }

  /**
    * <p>
    * Whether or not this is a scriptPubKey representing a P2SH output. In such outputs, the logic that
    * controls reclamation is not actually in the output at all. Instead there's just a hash, and it's up to the
    * spending input to provide a program matching that hash.
    * </p>
    * <p>
    * P2SH is described by <a href="https://github.com/bitcoin/bips/blob/master/bip-0016.mediawiki">BIP16</a>.
    * </p>
    */
  def isPayToScriptHash(script: Script): Boolean = {
    val chunks = script.getChunks
    // We check for the effective serialized form because BIP16 defines a P2SH output using an exact byte
    // template, not the logical program structure. Thus you can have two programs that look identical when
    // printed out but one is a P2SH script and the other isn't! :(
    // We explicitly test that the op code used to load the 20 bytes is 0x14 and not something logically
    // equivalent like {@code OP_HASH160 OP_PUSHDATA1 0x14 <20 bytes of script hash> OP_EQUAL}
    if (chunks.size != 3) return false
    if (!chunks.get(0).equalsOpCode(OP_HASH160)) return false
    val chunk1 = chunks.get(1)
    if (chunk1.opcode != 0x14) return false
    val chunk1data = chunk1.data
    if (chunk1data == null) return false
    if (chunk1data.length != LEGACY_ADDRESS_LENGTH) return false
    if (!chunks.get(2).equalsOpCode(OP_EQUAL)) return false
    true
  }

  /**
    * Returns true if this script is of the form {@code <pubkey> OP_CHECKSIG}. This form was originally intended for transactions
    * where the peers talked to each other directly via TCP/IP, but has fallen out of favor with time due to that mode
    * of operation being susceptible to man-in-the-middle attacks. It is still used in coinbase outputs and can be
    * useful more exotic types of transaction, but today most payments are to addresses.
    */
  def isPayToPubKey(script: Script): Boolean = {
    val chunks = script.getChunks
    if (chunks.size != 2) return false
    val chunk0 = chunks.get(0)
    if (chunk0.isOpCode) return false
    val chunk0data = chunk0.data
    if (chunk0data == null) return false
    if (chunk0data.length <= 1) return false
    if (!chunks.get(1).equalsOpCode(OP_CHECKSIG)) return false
    true
  }

  /**
    * Returns true if this script is of the form {@code OP_0 <hash>}. This can either be a P2WPKH or P2WSH scriptPubKey. These
    * two script types were introduced with segwit.
    */
  def isPayToWitnessHash(script: Script): Boolean = {
    val chunks = script.getChunks
    if (chunks.size != 2) return false
    if (!chunks.get(0).equalsOpCode(OP_0)) return false
    val chunk1data = chunks.get(1).data
    if (chunk1data == null) return false
    if (chunk1data.length != WITNESS_PROGRAM_LENGTH_PKH && chunk1data.length != WITNESS_PROGRAM_LENGTH_SH) return false
    true
  }

  /**
    * Returns true if this script is of the form {@code OP_0 <hash>} and hash is 20 bytes long. This can only be a P2WPKH
    * scriptPubKey. This script type was introduced with segwit.
    */
  def isPayToWitnessPubKeyHash(script: Script): Boolean = {
    if (!isPayToWitnessHash(script)) return false
    val chunks = script.getChunks
    if (!chunks.get(0).equalsOpCode(OP_0)) return false
    val chunk1data = chunks.get(1).data
    chunk1data != null && chunk1data.length == WITNESS_PROGRAM_LENGTH_PKH
  }

  /**
    * Returns true if this script is of the form {@code OP_0 <hash>} and hash is 32 bytes long. This can only be a P2WSH
    * scriptPubKey. This script type was introduced with segwit.
    */
  def isPayToWitnessScriptHash(script: Script): Boolean = {
    if (!isPayToWitnessHash(script)) return false
    val chunks = script.getChunks
    if (!chunks.get(0).equalsOpCode(OP_0)) return false
    val chunk1data = chunks.get(1).data
    chunk1data != null && chunk1data.length == WITNESS_PROGRAM_LENGTH_SH
  }


  def decodeFromOpN(opcode: Int): Int = {
    checkArgument((opcode == OP_0 || opcode == OP_1NEGATE) || (opcode >= OP_1 && opcode <= OP_16),
      "decodeFromOpN called on non OP_N opcode: %s", ScriptOpCodes.getOpCodeName(opcode))
    if (opcode == OP_0) 0
    else if (opcode == OP_1NEGATE) -1
    else opcode + 1 - OP_1
  }

  /**
    * Returns whether this script matches the format used for multisig outputs:
    * {@code [n] [keys...] [m] CHECKMULTISIG}
    */
  def isSentToMultisig(script: Script): Boolean = {
    val chunks = script.getChunks
    if (chunks.size < 4) return false
    val chunk = chunks.get(chunks.size - 1)
    // Must end in OP_CHECKMULTISIG[VERIFY].
    if (!chunk.isOpCode) return false
    if (!chunk.equalsOpCode(OP_CHECKMULTISIG) || chunk.equalsOpCode(OP_CHECKMULTISIGVERIFY)) return false
    try { // Second to last chunk must be an OP_N opcode and there should be that many data chunks (keys).
      val m = chunks.get(chunks.size - 2)
      if (!m.isOpCode) return false

      val numKeys = decodeFromOpN(m.opcode)
      if (numKeys < 1 || (chunks.size != 3 + numKeys)) return false
      var i = 1
      while ( {
        i < chunks.size - 2
      }) {
        if (chunks.get(i).isOpCode) return false

        {
          i += 1; i - 1
        }
      }
      // First chunk must be an OP_N opcode too.
      if (decodeFromOpN(chunks.get(0).opcode) < 1) return false
    } catch {
      case e: IllegalArgumentException =>
        return false // Not an OP_N opcode.

    }
    true
  }

  /**
    * Returns whether this script is using OP_RETURN to store arbitrary data.
    */
  def isOpReturn(script: Script): Boolean = {
    val chunks = script.getChunks
    chunks.size > 0 && chunks.get(0).equalsOpCode(ScriptOpCodes.OP_RETURN)
  }

}
 
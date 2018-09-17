package net.santiment.util

case class Migration(name:String, up:()=>Unit, clean:()=>Unit)

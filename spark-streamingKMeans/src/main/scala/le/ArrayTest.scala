package le

object ArrayTest {

  class DenStreamOffline(){

    var tag:Array[Int] = Array()

    def setArray(i:Int):Unit = {

      tag = new Array[Int](i)
      tag(0) = 1


    }
    def getArray() :Int = {
      tag(0)
    }



  }

  def main(args:Array[String]): Unit ={


    var t = new DenStreamOffline()
    t.setArray(2)
    println(t.getArray())




  }
}

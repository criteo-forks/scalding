package com.twitter.scalding.commons.source

import com.backtype.hadoop.pail.DefaultPailStructure
import com.twitter.scalding.TupleConverter
import org.scalatest.{ Matchers, WordSpec }

class PailSourceSpec extends WordSpec with Matchers {

  "A PailSource" should {

    "wire a default LowPriorityTupleConverter when none is in upper context" in {

      val a = PailSource.source("/", new DefaultPailStructure, Array(List(".")))

      a.converter.arity shouldBe 1
    }

    "wire a custom TupleSetter when defined" in {

      lazy implicit val customTupleConverter = TupleConverter.build(2) {
        te => te.getObject(1).asInstanceOf[Array[Byte]]
      }

      val a = PailSource.source("/", new DefaultPailStructure, Array(List(".")))

      a.converter.arity shouldBe 2
    }
  }
}

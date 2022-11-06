package io.su3.gkv.mesh.config

import com.google.gson.Gson

object MeshServiceConfig {
  lazy val serviceConfig: java.util.HashMap[String, ?] =
    Gson().fromJson(
      """{
        |  "methodConfig": [
        |    {
        |      "name": [
        |        {
        |          "service": "io.su3.gkv.mesh.proto.s2s.Mesh"
        |        }
        |      ],
        |      "retryPolicy": {
        |        "maxAttempts": 10,
        |        "initialBackoff": "0.1s",
        |        "maxBackoff": "10s",
        |        "backoffMultiplier": 2,
        |        "retryableStatusCodes": [
        |          "UNAVAILABLE",
        |          "DEADLINE_EXCEEDED"
        |        ]
        |      }
        |    }
        |  ]
        |}""".stripMargin,
      classOf[java.util.HashMap[String, ?]]
    )
}

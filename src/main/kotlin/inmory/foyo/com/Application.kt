package inmory.foyo.com

import io.ktor.server.application.*
import inmory.foyo.com.plugins.*

fun main(args: Array<String>): Unit =
    io.ktor.server.netty.EngineMain.main(args)

@Suppress("unused") // application.conf references the main function. This annotation prevents the IDE from marking it as unused.
fun Application.module() {
    configureHTTP()
    configureSerialization()
    configureDatabases()
    configureSecurity()
    configureRouting()
}

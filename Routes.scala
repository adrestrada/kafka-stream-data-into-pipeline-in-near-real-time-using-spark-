package ca.mcit.bigdata.kafka

case class Routes(routeId: String,
                  routeLongName: String,
                  routeColor: String)
object Routes {
  def toCsv(routes: Routes): String = {
    s"${routes.routeId},${routes.routeLongName},${routes.routeColor}"
  }
}

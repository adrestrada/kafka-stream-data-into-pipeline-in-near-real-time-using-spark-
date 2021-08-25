package ca.mcit.bigdata.kafka

case class TripsRoute(trips: Trips,
                      routes: Routes)
object TripsRoute {
  def toCsv(tripsRoute: TripsRoute): String = {
    s"${tripsRoute.trips},${tripsRoute.routes}"
  }
}

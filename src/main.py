import sched
import time

import nemweb
import influx

from load_env import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

s = sched.scheduler(time.time, time.sleep)


def acquire_and_process_nemweb(influx_client):
    nemweb_load_page = nemweb.NemWebLoads("Reports/Current/Dispatch_SCADA/")
    nemweb_solar_page = nemweb.NemWebSolar("Reports/Current/ROOFTOP_PV/ACTUAL/")
    nemweb_demand_page = nemweb.NemWebDemand("Reports/Current/Operational_Demand/ACTUAL_HH/")

    nemweb_load_page.DownloadAndProcess()
    influx_client.write(INFLUX_BUCKET, INFLUX_ORG, nemweb_load_page.influx_points)
    nemweb_solar_page.DownloadAndProcess()
    influx_client.write(INFLUX_BUCKET, INFLUX_ORG, nemweb_solar_page.influx_points)
    nemweb_demand_page.DownloadAndProcess()
    influx_client.write(INFLUX_BUCKET, INFLUX_ORG, nemweb_demand_page.influx_points)

    s.enter(300, 1, acquire_and_process_nemweb, (influx_client,))


if __name__ == "__main__":
    influxClient = influx.influxDB(
        INFLUX_URL,
        INFLUX_TOKEN,
        INFLUX_ORG
    )

    s.enter(1, 1, acquire_and_process_nemweb, (influxClient,))
    s.run()


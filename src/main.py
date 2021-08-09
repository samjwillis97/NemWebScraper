import sched
import time

import nemweb
import influx

from load_env import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

s = sched.scheduler(time.time, time.sleep)

def acquire_and_process_nemweb(influxClient):
    NemWebLoadPage = nemweb.NemWebLoads("Reports/Current/Dispatch_SCADA/")
    NemWebPricePage = nemweb.NemWebPriceAndGen("Reports/CURRENT/TradingIS_Reports/")

    NemWebLoadPage.DownloadAndProcess()
    influxClient.write(INFLUX_BUCKET, INFLUX_ORG, NemWebLoadPage.influx_points)

    NemWebPricePage.DownloadAndProcess()
    influxClient.write(INFLUX_BUCKET, INFLUX_ORG, NemWebPricePage.influx_points)

    s.enter(300, 1, acquire_and_process_nemweb, (influxClient,))


if __name__ == "__main__":
    influxClient = influx.influxDB(
        INFLUX_URL,
        INFLUX_TOKEN,
        INFLUX_ORG
    )

    s.enter(1, 1, acquire_and_process_nemweb, (influxClient,))
    s.run()


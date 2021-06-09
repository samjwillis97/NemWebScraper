import nemweb
import influx

from load_env import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

if __name__ == "__main__":
    influxClient = influx.influxDB(
        INFLUX_URL,
        INFLUX_TOKEN,
        INFLUX_ORG
    )

    NemWebLoadPage = nemweb.NemWebLoads("Reports/Current/Dispatch_SCADA/")
    NemWebPricePage = nemweb.NemWebPriceAndGen("Reports/CURRENT/TradingIS_Reports/")

    NemWebLoadPage.DownloadAndProcess()
    influxClient.write(INFLUX_BUCKET, INFLUX_ORG, NemWebLoadPage.influx_points)

    NemWebPricePage.DownloadAndProcess()
    influxClient.write(INFLUX_BUCKET, INFLUX_ORG, NemWebPricePage.influx_points)

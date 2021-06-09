# NemWeb Scraper

A simple python script to scrape the unit loads of power stations in Australian from nemweb.com.au.

Intended to be used in a docker container and feed to InfluxDB container.

## Docker Compose

```yaml
nemweb:
    image: samjwillis/nemweb_scraper:latest
    container_name: nemweb
    networks:
        - default
    environment:
        - DEBUG=
        - CLEAR_DBM_ON_START=
        - INFLUX_ORG=
        - INFLUX_BUCKET=
        - INFLUX_URL=
        - INFLUX_TOKEN=
```
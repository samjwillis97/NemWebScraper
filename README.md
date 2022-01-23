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
        - SQLITE_FILE=
        - INFLUX_ORG=
        - INFLUX_BUCKET=
        - INFLUX_URL=
        - INFLUX_TOKEN=
```

## Unit Identification

See: 
    - http://www.whit.com.au/aemo-registered-participants/
    - https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls
Make this into a map
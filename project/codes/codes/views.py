from google.cloud import bigquery

client = bigquery.Client()

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    from test import test
    test()
    def big_query(query):
      update_query = client.query(query)
      data = update_query.result()
      return 

    timeZone={'262796536':{'countries':('ar', 'br', 'cl', 'co', 'mx', 'pe', 'us','ca'),
                           'ar':"America/Argentina/Buenos_Aires",'br':"America/Sao_Paulo",
                           'cl': "America/Santiago",'co': "America/Bogota",
                           'mx': "America/Mexico_City",'pe': "America/Lima",
                           'us': "America/Los_Angeles",'ca':"Canada/Eastern"},
            'Apej':{'countries':('cn', 'hk', 'in', 'id', 'kr', 'my', 'sg', 'th', 'au','nz'),
                    'cn': "Asia/Shanghai",'hk': "Asia/Hong_Kong",
                    'in': "Asia/Kolkata",'id': "Asia/Jakarta",
                    'kr': "Asia/Seoul",'my': "Asia/Kuala_Lumpur",
                    'sg': "Asia/Singapore",'th': "Asia/Bangkok",
                    'au': "Australia/Sydney",'nz': "Pacific/Auckland"},
            'EMEA':{'countries':('uk','fr','de','it','es','nl','pl','ch','be','se'),
                'uk': "Europe/London",'fr': "Europe/Paris",
                'de': "Europe/Berlin",'it': "Europe/Rome",
                'es': "Europe/Madrid",'se': "Europe/Stockholm",
                'nl': "Europe/Amsterdam",'pl': "Europe/Warsaw",
                'ch': "Europe/Zurich",'be': "Europe/Brussels"}
          }

    def viewsQuery(regionId,countryView,timezone):
        timeZoneData=timezone[regionId]
        countries=timeZoneData['countries']
        caseWhenStatement=''
        for country in countries:
            caseWhenStatement+=f'''
                WHEN Country = '{country}' THEN DATE(TIMESTAMP_SECONDS(visitStartTime), '{timeZoneData[country]}')'''
        query=f''' 
         CREATE OR REPLACE TABLE `ga360-bigquery-sandbox.MVP_new_GA4_test.US_views_test` AS
         (with region as (
          SELECT 
            t1.*, 
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS visitStartTime
          FROM 
            `ga360-bigquery-sandbox.analytics_{regionId}.events_*` t1
          WHERE _TABLE_SUFFIX BETWEEN '20230603' AND REPLACE(CAST(CURRENT_DATE() AS STRING), '-', '')
        )
        SELECT
          region.*,
          countries.Country,
          countries.Local_Date,
          countries.UTC_Date
        FROM
          region
        INNER JOIN
          (SELECT * 
          FROM 
            ((WITH Countries AS (
            SELECT 
              user_pseudo_id,
              (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS visitStartTime,
              ARRAY_REVERSE(ARRAY_AGG((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'v01')))[OFFSET(0)] AS Country
            FROM
            `ga360-bigquery-sandbox.analytics_{regionId}.events_*`
            WHERE _TABLE_SUFFIX BETWEEN '20230603'and REPLACE(CAST(CURRENT_DATE() AS STRING), '-', '')
            GROUP BY
            user_pseudo_id,
            visitStartTime
          )
          SELECT
            user_pseudo_id,
            visitStartTime,
            Country,
            CASE {caseWhenStatement}
            END AS Local_Date,
            DATE(TIMESTAMP_SECONDS(visitStartTime)) AS UTC_Date
          FROM
            Countries
          WHERE
            Country IN {countries})
            ) 
          WHERE Country = '{countryView}') countries
        ON
          region.user_pseudo_id = countries.user_pseudo_id AND region.visitStartTime = countries.visitStartTime
        )'''
        big_query(query)

        return

    viewsQuery('262796536','us',timeZone)


# def big_query(query):
#     update_query = client.query(query)
#     data = update_query.result()
#     return data

# timeZone={'262796536':{'countries':('ar', 'br', 'cl', 'co', 'mx', 'pe', 'us','ca'),
#                        'ar':"America/Argentina/Buenos_Aires",'br':"America/Sao_Paulo",
#                        'cl': "America/Santiago",'co': "America/Bogota",
#                        'mx': "America/Mexico_City",'pe': "America/Lima",
#                        'us': "America/Los_Angeles",'ca':"Canada/Eastern"},
#         'Apej':{'countries':('cn', 'hk', 'in', 'id', 'kr', 'my', 'sg', 'th', 'au','nz'),
#                 'cn': "Asia/Shanghai",'hk': "Asia/Hong_Kong",
#                 'in': "Asia/Kolkata",'id': "Asia/Jakarta",
#                 'kr': "Asia/Seoul",'my': "Asia/Kuala_Lumpur",
#                 'sg': "Asia/Singapore",'th': "Asia/Bangkok",
#                 'au': "Australia/Sydney",'nz': "Pacific/Auckland"},
#         'EMEA':{'countries':('uk','fr','de','it','es','nl','pl','ch','be','se'),
#                 'uk': "Europe/London",'fr': "Europe/Paris",
#                 'de': "Europe/Berlin",'it': "Europe/Rome",
#                 'es': "Europe/Madrid",'se': "Europe/Stockholm",
#                 'nl': "Europe/Amsterdam",'pl': "Europe/Warsaw",
#                 'ch': "Europe/Zurich",'be': "Europe/Brussels"}
#           }

# def viewsQuery(regionId,countryView,timezone):
#     timeZoneData=timezone[regionId]
#     countries=timeZoneData['countries']
#     caseWhenStatement=''
#     for country in countries:
#         caseWhenStatement+=f'''
#             WHEN Country = '{country}' THEN DATE(TIMESTAMP_SECONDS(visitStartTime), '{timeZoneData[country]}')'''
#     query=f''' 
#      CREATE OR REPLACE TABLE `ga360-bigquery-sandbox.MVP_new_GA4_test.US_views_test` AS
#      (with region as (
#       SELECT 
#         t1.*, 
#         (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS visitStartTime
#       FROM 
#         `ga360-bigquery-sandbox.analytics_{regionId}.events_*` t1
#       WHERE _TABLE_SUFFIX BETWEEN '20230603' AND REPLACE(CAST(CURRENT_DATE() AS STRING), '-', '')
#     )
#     SELECT
#       region.*,
#       countries.Country,
#       countries.Local_Date,
#       countries.UTC_Date
#     FROM
#       region
#     INNER JOIN
#       (SELECT * 
#       FROM 
#         ((WITH Countries AS (
#         SELECT 
#           user_pseudo_id,
#           (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS visitStartTime,
#           ARRAY_REVERSE(ARRAY_AGG((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'v01')))[OFFSET(0)] AS Country
#         FROM
#         `ga360-bigquery-sandbox.analytics_{regionId}.events_*`
#         WHERE _TABLE_SUFFIX BETWEEN '20230603'and REPLACE(CAST(CURRENT_DATE() AS STRING), '-', '')
#         GROUP BY
#         user_pseudo_id,
#         visitStartTime
#       )
#       SELECT
#         user_pseudo_id,
#         visitStartTime,
#         Country,
#         CASE {caseWhenStatement}
#         END AS Local_Date,
#         DATE(TIMESTAMP_SECONDS(visitStartTime)) AS UTC_Date
#       FROM
#         Countries
#       WHERE
#         Country IN {countries})
#         ) 
#       WHERE Country = '{countryView}') countries
#     ON
#       region.user_pseudo_id = countries.user_pseudo_id AND region.visitStartTime = countries.visitStartTime
#     )'''
#     big_query(query)

#     return

# viewsQuery('262796536','us',timeZone)

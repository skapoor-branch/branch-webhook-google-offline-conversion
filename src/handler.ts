import { APIGatewayProxyHandler } from 'aws-lambda'
import 'source-map-support/register'
import { GoogleAdsApi, CustomerInstance } from 'google-ads-api'
import moment from 'moment'

export const run: APIGatewayProxyHandler = async (event, _context) => {
  const { body } = event
  if (!body) {
    return {
      statusCode: 500,
      body: JSON.stringify({
          message: 'body not provided in request',
        }, null, 2),
    }
  }
  const data = JSON.parse(body)
  if (
    !data.last_attributed_touch_data ||
    data.last_attributed_touch_data['$3p'] !== 'a_google_adwords' ||
    !data.last_attributed_touch_data.gclid ||
    data.last_attributed_touch_data.gclid.length === 0
  ) {
    return {
      statusCode: 200,
      body: JSON.stringify( {
          message: 'Ad network not a_google_adwords and/or gclid not found',
        }, null, 2),
    }
  }

  //unique the list of gclids, because there can be duplicates
  const gclids = [...new Set(data.last_attributed_touch_data.gclid as string[])]
  const {
    name,
    timestamp,
    event_data,
  }: {
    timestamp: number
    name: string
    event_data?: {
      currency?: string
      revenue?: number
      transaction_id?: string
    }
  } = data
  console.debug(`gclids: ${gclids.join(', ')}`)

  const conversions = gclids.map((gclid) => {
    let conversion = {
      gclid,
      conversion_action: name,
      conversion_date_time: moment(timestamp).format('yyyy-mm-dd hh:mm:ss+|-hh:mm'),
    }
    if (!event_data) {
      return conversion
    }
    const { currency: currency_code, revenue: conversion_value, transaction_id: order_id } = event_data
    return {
      ...conversion,
      conversion_value,
      currency_code,
      order_id,
    }
  })
  // Passing in a single entity to create
  const queryParameters = event.queryStringParameters
  if (!queryParameters) {
    return {
      statusCode: 500,
      body: JSON.stringify({
          message: 'no authentication parameters provided, unable to upload conversion click',
        }, null, 2),
    }
  }
  console.debug(`Uploading conversions: ${JSON.stringify(conversions)}`)
  const createAction = await customer(event.queryStringParameters)
  .conversionUploads
  .uploadClickConversions(conversions, {
    validate_only: queryParameters.validate_only === 'true',
  })
  console.debug(`${JSON.stringify(createAction)}`)
  if (!!createAction.partial_failure_error) {
    return {
      statusCode: 500,
      body: JSON.stringify(createAction.partial_failure_error),
    }
  }
  return {
    statusCode: 200,
    body: JSON.stringify(createAction.results),
  }
}

function customer(queryParameters: any): CustomerInstance {
  const { } = queryParameters
  const client = new GoogleAdsApi({
    ...queryParameters
  })
  return client.Customer({
    ...queryParameters
  })
}
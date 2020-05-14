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
    const options = {
      validate_only: queryParameters.validate_only === 'true',
      partial_failure: true
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
  const customer = customerAPI(event.queryStringParameters)


  const offlineActionName = `branch_offline_${name}`
  const actions = await customer.conversionActions.list()
  let resource_name = actions.find(action => {
    return action.conversion_action.name === offlineActionName
  })?.conversion_action.resource_name
  if (!resource_name) {
    resource_name = await createAction(offlineActionName, customer)
  }
  if (!resource_name) {
    throw Error('Cannot find or create a conversion action')
  }
  console.debug(`conversion action: ${JSON.stringify(resource_name)}`)

  const conversions = gclids.map((gclid) => {
    let conversion = {
      gclid,
      conversion_action: resource_name,
      conversion_date_time: moment(timestamp).format('YYYY-MM-DD hh:mm:ssZ'),
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

  console.debug(`Uploading conversions: ${JSON.stringify(conversions)}`)
  const createClicks = await customer.conversionUploads.uploadClickConversions(conversions, options)
  console.debug(`Create conversion result: ${JSON.stringify(createClicks)}`)
  if (!!createClicks.partial_failure_error) {
    return {
      statusCode: 500,
      body: JSON.stringify(createClicks.partial_failure_error),
    }
  }
  return {
    statusCode: 200,
    body: JSON.stringify(createClicks.results),
  }
}

const createAction = async (name: string, customer: CustomerInstance) => {
  const conversion_action = {
    name,
    type: 7,
    status: 2,
    value_settings: {
      default_value:0,
      default_currency_code:'USD'
    },
    category: 4,
    include_in_conversions_metric: true,
    counting_type: 3
    // status?: ConversionActionStatus;
    // category?: ConversionActionCategory;
    // owner_customer?: string;
    // include_in_conversions_metric?: boolean;
    // click_through_lookback_window_days?: number;
    // view_through_lookback_window_days?: number;
    // value_settings?: ValueSettings;
    // counting_type?: ConversionActionCountingType;
    // attribution_model_settings?: AttributionModelSettings;
    // tag_snippets?: TagSnippet[];
    // phone_call_duration_seconds?: number;
    // app_id?: string;
  }
  
  // Passing in a single entity to create
  const actionResponse = await customer.conversionActions.create(conversion_action, {
    partial_failure: true
  })
  if (!!actionResponse.partial_failure_error) {
    throw new Error(`Unable to create action: ${name} details: ${actionResponse.partial_failure_error}`)
  }
  console.debug(`actionResponse: ${JSON.stringify(actionResponse.results[0])}`)
  return actionResponse.results[0]
}

function customerAPI(queryParameters: any): CustomerInstance {
  const { } = queryParameters
  const client = new GoogleAdsApi({
    ...queryParameters
  })
  return client.Customer({
    ...queryParameters
  })
}
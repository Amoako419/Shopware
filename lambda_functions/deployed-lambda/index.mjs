import { Firehose } from '@aws-sdk/client-firehose';
import axios from 'axios';

// Initialize the Firehose client
const firehose = new Firehose({ region: process.env.AWS_REGION });

/**
 * Lambda function that acts as a proxy between API Gateway and Kinesis Firehose
 * 
 * @param {Object} event - API Gateway event
 * @param {Object} context - Lambda context
 * @returns {Object} - API Gateway response
 */
export const handler = async (event, context) => {
    console.log('Event received:', JSON.stringify(event, null, 2));
    
    try {
        // Extract request parameters
        const requestBody = event.body ? JSON.parse(event.body) : {};
        const queryParams = event.queryStringParameters || {};
        
        // Configuration values that should be environment variables in production
        const EXTERNAL_API_URL = process.env.EXTERNAL_API_URL;
        const FIREHOSE_DELIVERY_STREAM_NAME = process.env.FIREHOSE_DELIVERY_STREAM_NAME;
        
        if (!EXTERNAL_API_URL) {
            throw new Error('External API URL is not configured');
        }
        
        if (!FIREHOSE_DELIVERY_STREAM_NAME) {
            throw new Error('Firehose delivery stream name is not configured');
        }
        
        // Call the external API
        console.log(`Calling external API: ${EXTERNAL_API_URL}`);
        const apiResponse = await callExternalApi(
            EXTERNAL_API_URL, 
            requestBody, 
            queryParams, 
            event.headers || {}
        );
        
        // Process the API response
        const processedData = processApiResponse(apiResponse, event);
        
        // Stream the processed data to Kinesis Firehose
        await streamToFirehose(processedData, FIREHOSE_DELIVERY_STREAM_NAME);
        
        // Return a successful response to API Gateway
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                message: 'Data successfully processed and sent to Firehose',
                requestId: context.awsRequestId
            })
        };
    } catch (error) {
        console.error('Error:', error);
        
        // Return an error response to API Gateway
        return {
            statusCode: error.statusCode || 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                message: `Error: ${error.message}`,
                requestId: context.awsRequestId
            })
        };
    }
};

/**
 * Calls the external API with appropriate parameters
 * 
 * @param {string} url - The external API URL
 * @param {Object} body - The request body
 * @param {Object} queryParams - URL query parameters
 * @param {Object} headers - HTTP headers to include
 * @returns {Object} - The API response
 */
async function callExternalApi(url, body, queryParams, headers) {
    try {
        // Include relevant headers from the original request
        // But filter out AWS-specific headers
        const apiHeaders = {};
        const headerWhitelist = ['content-type', 'authorization', 'user-agent'];
        
        Object.keys(headers).forEach(key => {
            const lowerKey = key.toLowerCase();
            if (headerWhitelist.includes(lowerKey) && !lowerKey.startsWith('x-amz-')) {
                apiHeaders[key] = headers[key];
            }
        });
        
        // Construct the request config
        const requestConfig = {
            method: 'POST', // Change based on requirements
            url: url,
            headers: apiHeaders,
            data: body,
            params: queryParams
        };
        
        console.log('API request config:', JSON.stringify(requestConfig, null, 2));
        const response = await axios(requestConfig);
        
        console.log('API response status:', response.status);
        return response.data;
    } catch (error) {
        console.error('External API error:', error.message);
        if (error.response) {
            console.error('Response data:', error.response.data);
            console.error('Response status:', error.response.status);
        }
        throw new Error(`External API call failed: ${error.message}`);
    }
}

/**
 * Process the API response data before sending to Firehose
 * 
 * @param {Object} apiResponse - The response from the external API
 * @param {Object} originalEvent - The original Lambda event
 * @returns {Object} - Processed data ready for Firehose
 */
function processApiResponse(apiResponse, originalEvent) {
    // Add any necessary data transformations here
    // This is an example that adds metadata and flattens certain fields
    
    const timestamp = new Date().toISOString();
    const requestId = originalEvent.requestContext?.requestId || 'unknown';
    
    // Example processing - customize based on your requirements
    const processedData = {
        timestamp,
        requestId,
        source: 'api-gateway',
        data: apiResponse,
        // Add other metadata or transformations as needed
    };
    
    return processedData;
}

/**
 * Streams the processed data to Kinesis Firehose
 * 
 * @param {Object} data - The processed data to send
 * @param {string} deliveryStreamName - The name of the Firehose delivery stream
 * @returns {Object} - The Firehose PutRecord response
 */
async function streamToFirehose(data, deliveryStreamName) {
    // Convert the data to a string if it's an object
    const dataString = typeof data === 'string' ? data : JSON.stringify(data);
    
    // Convert the string to a Buffer which is required by Firehose
    const record = {
        DeliveryStreamName: deliveryStreamName,
        Record: {
            Data: Buffer.from(dataString)
        }
    };
    
    console.log(`Sending data to Firehose stream: ${deliveryStreamName}`);
    
    try {
        const response = await firehose.putRecord(record);
        console.log('Data sent to Firehose successfully:', response);
        return response;
    } catch (error) {
        console.error('Error sending data to Firehose:', error);
        throw new Error(`Failed to send data to Firehose: ${error.message}`);
    }
}
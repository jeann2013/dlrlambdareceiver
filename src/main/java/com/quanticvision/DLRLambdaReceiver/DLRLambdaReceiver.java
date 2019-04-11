package com.quanticvision.DLRLambdaReceiver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

/**
 * 
 * @author QuanticVision
 * @see Clase para Procesar los endpoint y direccionar a la cola corespondiente
 *
 */
public class DLRLambdaReceiver implements RequestStreamHandler {
	String debug = System.getenv("debug");

	public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");

		JSONObject payload;
		String result;

		try {
			payload = (JSONObject) (new JSONParser()).parse(reader);
			result = processPayload(payload);
			try {
				writer.write(result);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Error!!: " + e.getMessage());
			}

		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	public String processPayload(JSONObject payload) {
		String channel = "";
		String client = "";
		String provider = "";
		String message = "";
		String queueUrl = "";

		// extrae los Parametros de pathParameters
		if (payload.get("pathParameters") != null) {
			JSONObject pathParameters = (JSONObject) payload.get("pathParameters");
			if (pathParameters.get("channel") != null) {
				channel = (String) pathParameters.get("channel");
			}
			if (pathParameters.get("client") != null) {
				client = (String) pathParameters.get("client");
			}
			if (pathParameters.get("provider") != null) {
				provider = (String) pathParameters.get("provider");
			}
		}

		// arma el URL para la cola a la cual se va a escribir.
		queueUrl = System.getenv("SQSBaseUrl") + client + "-" + channel + "-" + provider + "-dlr";

		if (payload.get("body") != null) {
			message = payload.get("body").toString();
		}

		if (debug.equals("Yes")) {
			System.out.println(payload.toJSONString());
			System.out.println("Queue Url: " + queueUrl);
		}

		return sendMessageToQueue(queueUrl, message);

	}

	/**
	 * 
	 * @param queueUrl
	 * @param message
	 * @throws UnsupportedEncodingException
	 */
	public String sendMessageToQueue(String queueUrl, String message) {
		AmazonSQSClient sqs = new AmazonSQSClient();
		
		Map<String, Object> response = new HashMap<String, Object>();
		Map<String, Object> header = new HashMap<String, Object>();
		Map<String, Object> body = new HashMap<String, Object>();		

		if (debug.equals("Yes")) {
			System.out.println("Writing to Queue: " + queueUrl);
		}
		SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(message);
		SendMessageResult messageResult = sqs.sendMessage(send_msg_request);
		String mr = messageResult.toString();
		if (debug.equals("Yes")) {
			System.out.println("Results: " + mr);
		}

		header.put("Provided-By", "Enterprice");		
		body.put("Message-Result", mr);
		body.put("State", "Sent data!");
		response.put("isBase64Encoded", "false");
		response.put("statusCode", "200");
		response.put("headers", header);
		response.put("body", JSONObject.toJSONString(body));		
		

		if (debug.equals("Yes")) {
			System.out.println("API Response : " + JSONObject.toJSONString(response));
		}
		
		return JSONObject.toJSONString(response);

	}
	
}

import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

let docClient = null;
let AMAZON_DYNAMODB_TABLE = null;

export const initializeClient = (event = {}) => {
  const { AMAZON_ACCESS_KEY_ID, AMAZON_SECRET_ACCESS_KEY, AMAZON_DYNAMODB_TABLE: eventTable } = event.credentials || {};

  AMAZON_DYNAMODB_TABLE = eventTable || process.env.AMAZON_DYNAMODB_TABLE;

  const ddbClientOptions = {};

  if (process.env.AMAZON_REGION) {
    ddbClientOptions.region = process.env.AMAZON_REGION;
  }
  if (AMAZON_ACCESS_KEY_ID && AMAZON_SECRET_ACCESS_KEY) {
    ddbClientOptions.credentials = {
      accessKeyId: AMAZON_ACCESS_KEY_ID,
      secretAccessKey: AMAZON_SECRET_ACCESS_KEY,
    };
  }

  const ddbClient = new DynamoDBClient(ddbClientOptions);
  docClient = DynamoDBDocumentClient.from(ddbClient);
};

const incrementClicks = async (linkId) => {
  const params = {
    TableName: AMAZON_DYNAMODB_TABLE,
    Key: { PK: linkId },
    UpdateExpression: "SET Clicks = if_not_exists(Clicks, :zero) + :inc",
    ExpressionAttributeValues: {
      ":inc": 1,
      ":zero": 0,
    },
  };
  try {
    await docClient.send(new UpdateCommand(params));
  } catch (error) {
    console.error(`Error incrementing Clicks for linkId ${linkId}:`, error);
  }
};

export const getRedirectUrl = async (event = {}) => {
  initializeClient(event);

  let path = null;
  if (event.rawEvent?.rawPath) {
    path = event.rawEvent.rawPath;
  } else if (event.rawEvent?.requestContext?.http?.path) {
    path = event.rawEvent.requestContext.http.path;
  } else if (event.rawPath) {
    path = event.rawPath;
  } else if (event.requestContext?.http?.path) {
    path = event.requestContext.http.path;
  }

  if (!path || path === "/") {
    return null;
  }

  const parts = path.replace(/^\//, "").split("/");
  const [firstPart, secondPart] = parts;

  let account = null;
  let linkId = null;

  if (firstPart && firstPart.startsWith(":")) {
    account = firstPart.replace(/^:/, "");
    linkId = secondPart;
  } else {
    linkId = firstPart;
  }

  if (!account || !linkId || linkId.length < 3) {
    return null;
  }

  const params = {
    TableName: AMAZON_DYNAMODB_TABLE,
    Key: {
      PK: account ? `${account.toUpperCase()}#${linkId}` : linkId,
    },
  };

  try {
    const result = await docClient.send(new GetCommand(params));

    if (result?.Item?.Url) {
      if ("Clicks" in result.Item) {
        incrementClicks(linkId);
      }
      return result.Item.Url;
    }

    return null;
  } catch (error) {
    console.error(`Error getting redirect URL for linkId ${linkId}`);
    throw error;
  }
};

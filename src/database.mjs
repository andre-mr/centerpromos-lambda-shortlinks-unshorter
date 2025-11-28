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

  const BOT_USER_AGENTS = [
    "Googlebot",
    "Google-InspectionTool",
    "AdsBot-Google",
    "bingbot",
    "YandexBot",
    "Baiduspider",
    "DuckDuckBot",
    "Applebot",
    "PetalBot",
    "Bytespider",
    "Sogou",
    "Exabot",
    "Qwantify",
    "SeznamBot",
    "facebookexternalhit",
    "Facebot",
    "Twitterbot",
    "LinkedInBot",
    "Slackbot-LinkExpanding",
    "Discordbot",
    "WhatsApp/",
    "TelegramBot",
    "SkypeUriPreview",
    "GPTBot",
    "ClaudeBot",
    "PerplexityBot",
    "CCBot",
    "ia_archiver",
  ];
  const headers = event.rawEvent?.headers ?? event.headers ?? {};
  const requestHttp = event.rawEvent?.requestContext?.http ?? event.requestContext?.http;
  const userAgent =
    [headers["user-agent"], headers["x-user-agent"], requestHttp?.userAgent].find(
      (value) => typeof value === "string" && value.trim()
    ) ?? "";
  const normalizedUserAgent = userAgent.toLowerCase();
  const isBot = BOT_USER_AGENTS.some((bot) => normalizedUserAgent.includes(bot.toLowerCase()));

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

  const forwardedHostHeader = [headers["x-forwarded-host"], headers["X-Forwarded-Host"]].find(
    (value) => typeof value === "string" && value.trim()
  );
  const forwardedHost = forwardedHostHeader?.trim();

  const parts = path.replace(/^\//, "").split("/").filter(Boolean);
  const [firstPart, secondPart] = parts;

  const legacyAccount = firstPart?.startsWith(":") && secondPart ? firstPart.replace(/^:/, "") : null;
  const legacyLinkId = legacyAccount ? secondPart : null;
  const legacyPrimaryKey = legacyAccount && legacyLinkId ? `${legacyAccount.toUpperCase()}#${legacyLinkId}` : null;

  let account = null;
  let linkId = null;
  let primaryKey = null;

  if (forwardedHost) {
    linkId = firstPart;
    primaryKey = linkId ? `${forwardedHost}#${linkId}` : null;
  } else if (firstPart?.startsWith(":") && secondPart) {
    // Legacy fallback: account-prefixed paths "/:account/link" (to be removed once new host-based routing is stable)
    account = legacyAccount;
    linkId = legacyLinkId;
    primaryKey = legacyPrimaryKey;
  } else {
    linkId = firstPart;
    primaryKey = linkId ?? null;
  }

  if (!linkId || linkId.length < 3 || !primaryKey) {
    return null;
  }

  const params = {
    TableName: AMAZON_DYNAMODB_TABLE,
    Key: {
      PK: primaryKey,
    },
  };

  try {
    let result = await docClient.send(new GetCommand(params));
    let currentPrimaryKey = primaryKey;
    let currentLinkId = linkId;
    let currentAccount = account;

    if (!(result?.Item?.Url) && forwardedHost && legacyPrimaryKey && legacyLinkId?.length >= 3) {
      // Legacy fallback for host-based requests where the item is still stored with the old PK format
      currentPrimaryKey = legacyPrimaryKey;
      currentLinkId = legacyLinkId;
      currentAccount = legacyAccount;
      result = await docClient.send(
        new GetCommand({
          TableName: AMAZON_DYNAMODB_TABLE,
          Key: { PK: currentPrimaryKey },
        })
      );
    }

    if (result?.Item?.Url) {
      // Collect tracking params from queryStringParameters (fallback to rawQueryString)
      const TRACK_KEYS = ["fbclid", "gclid", "ttclid", "twclid"];

      // Prefer queryStringParameters; fallback to parsing rawQueryString
      const qsParams = event.rawEvent?.queryStringParameters ?? event.queryStringParameters;
      let collected = {};

      if (qsParams && typeof qsParams === "object") {
        for (const key of TRACK_KEYS) {
          const val = qsParams[key];
          if (val) collected[key] = val;
        }
      } else {
        const rawQs = event.rawEvent?.rawQueryString ?? event.rawQueryString;
        if (rawQs) {
          const sp = new URLSearchParams(rawQs);
          for (const key of TRACK_KEYS) {
            const v = sp.get(key);
            if (v) collected[key] = v;
          }
        }
      }

      if (Object.keys(collected).length) {
        try {
          const urlObj = new URL(result.Item.Url);
          for (const [k, v] of Object.entries(collected)) {
            urlObj.searchParams.set(k, v);
          }
          result.Item.Url = urlObj.toString();
        } catch (_) {
          const sep = result.Item.Url.includes("?") ? "&" : "?";
          const qs = new URLSearchParams(collected).toString();
          result.Item.Url = `${result.Item.Url}${sep}${qs}`;
        }
      }
      if ("Clicks" in result.Item && !isBot) {
        incrementClicks(currentPrimaryKey);
      }
      return result.Item.Url;
    }

    return null;
  } catch (error) {
    console.error(`Error getting redirect URL for linkId ${linkId}`);
    throw error;
  }
};

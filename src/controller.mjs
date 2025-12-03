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

export const incrementOfferClicks = async ({
  itemCampaign = "",
  itemDomain = "",
  offerID = "",
  accountID = "",
} = {}) => {
  if (!docClient) {
    console.error("DynamoDB client not initialized; skipping offer click increment.");
    return;
  }

  const normalizedCampaign = itemCampaign.replace(/\s+/g, "").trim().toUpperCase();
  const normalizedAccount = accountID.trim().toLowerCase();
  const isMultiAccount = process.env.MULTI_ACCOUNT === "true";
  const tableName = (isMultiAccount && normalizedAccount) || process.env.AMAZON_DYNAMODB_TABLE_DEFAULT || null;
  const domainsToCampaigns = JSON.parse(JSON.stringify(process.env.DOMAINS_TO_CAMPAIGNS) || "");
  const normalizedDomain = domainsToCampaigns?.[itemDomain] || "";
  if ((!normalizedCampaign && !normalizedDomain) || !offerID || !tableName) {
    console.error("Invalid parameters for incrementOfferClicks; skipping offer click increment.");
    return;
  }

  const offerPK = `OFFER#${normalizedCampaign || normalizedDomain}`;

  try {
    await docClient.send(
      new UpdateCommand({
        TableName: tableName,
        Key: {
          PK: offerPK,
          SK: offerID,
        },
        UpdateExpression: "SET Clicks = if_not_exists(Clicks, :initial) + :increment",
        ExpressionAttributeValues: {
          ":increment": 1,
          ":initial": 0,
        },
        ConditionExpression: "attribute_exists(SK)",
      })
    );
  } catch (error) {
    console.error("Error incrementing offer clicks:", error);
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
  const hasMultiAccount = process.env.MULTI_ACCOUNT === "true";
  const accountId = parts.length === 2 ? firstPart?.replace(/^:/, "") : null;
  const linkId = parts.length === 1 ? firstPart : parts.length === 2 ? secondPart : null;

  if (!linkId || linkId.length < 3) {
    return null;
  }

  // Build lookup order: host-scoped -> account-scoped (when enabled) -> link-only
  const candidates = [];
  if (forwardedHost) {
    candidates.push({ pk: `${forwardedHost}#${linkId}` });
  }
  if (hasMultiAccount && accountId) {
    candidates.push({ pk: `${accountId.toUpperCase()}#${linkId}` });
  }
  if (!hasMultiAccount) {
    candidates.push({ pk: linkId });
  }
  console.log("candidates", candidates);

  try {
    let result = null;
    let resolvedPrimaryKey = null;

    for (const candidate of candidates) {
      result = await docClient.send(
        new GetCommand({
          TableName: AMAZON_DYNAMODB_TABLE,
          Key: { PK: candidate.pk },
        })
      );
      if (result?.Item?.Url) {
        resolvedPrimaryKey = candidate.pk;
        break;
      }
    }

    if (result?.Item?.Url && resolvedPrimaryKey) {
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

      if (!isBot) {
        if (result.Item?.Clicks !== undefined) {
          incrementClicks(resolvedPrimaryKey);
        }
        const itemCampaign = result.Item?.Campaign;
        const itemDomain = result.Item?.Domain;
        const offerID = result.Item?.OfferID ?? result.Item?.OfferSK;
        if ((itemCampaign || itemDomain) && offerID) {
          incrementOfferClicks({
            itemCampaign,
            itemDomain,
            offerID,
            accountID: result.Item?.AccountID ?? "",
          });
        }
      }
      return result.Item.Url;
    }

    return null;
  } catch (error) {
    console.error(`Error getting redirect URL for linkId ${linkId}`);
    throw error;
  }
};

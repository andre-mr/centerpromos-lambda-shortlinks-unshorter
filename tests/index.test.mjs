import { jest } from "@jest/globals";
import { handler } from "../src/index.mjs";
import dotenv from "dotenv";
dotenv.config();

jest.mock("@aws-sdk/client-dynamodb", () => {
  return {
    DynamoDBClient: jest.fn().mockImplementation(() => ({
      send: jest.fn(),
    })),
  };
});

jest.mock("@aws-sdk/lib-dynamodb", () => {
  return {
    DynamoDBDocumentClient: {
      from: jest.fn().mockReturnValue({
        send: jest.fn().mockResolvedValue({ UnprocessedItems: {} }),
      }),
    },
    GetCommand: jest.fn().mockImplementation((params) => params),
  };
});

const credentials = {
  AMAZON_ACCESS_KEY_ID: process.env.AMAZON_ACCESS_KEY_ID,
  AMAZON_SECRET_ACCESS_KEY: process.env.AMAZON_SECRET_ACCESS_KEY,
  AMAZON_DYNAMODB_TABLE: process.env.AMAZON_DYNAMODB_TABLE,
};

describe("Lambda Handler Unshorter Tests", () => {
  beforeEach(() => {
    process.env.AMAZON_DYNAMODB_TABLE = process.env.AMAZON_DYNAMODB_TABLE;
    jest.clearAllMocks();
  });

  test("should successfully redirect when item exists", async () => {
    const mockEvent = {
      credentials,
      rawPath: "/:promodev/testid",
    };
    const response = await handler(mockEvent);
    expect(response.statusCode).toBe(302);
    expect(response.headers.Location).toBeDefined();
    expect(response.headers.Location).toBe("https://example.com");
  });

  test("should successfully redirect using x-forwarded-host as part of PK", async () => {
    const mockEvent = {
      credentials,
      rawPath: "/testid",
      headers: {
        "x-forwarded-host": "link.promodev.com",
      },
    };
    const response = await handler(mockEvent);
    expect(response.statusCode).toBe(302);
    expect(response.headers.Location).toBeDefined();
  });

  test("should return 404 for an invalid path", async () => {
    const mockEvent = {
      credentials,
      rawPath: "/invalidpath",
    };
    const response = await handler(mockEvent);
    expect(response.statusCode).toBe(404);
    expect(response.headers["Content-Type"]).toBe("text/html");
    expect(response.body).toContain("Link não encontrado");
  });

  test("should return 404 for a non-existent item", async () => {
    const mockEvent = {
      credentials,
      rawPath: "/:promodev/nonexistentid",
    };
    const response = await handler(mockEvent);
    expect(response.statusCode).toBe(404);
    expect(response.headers["Content-Type"]).toBe("text/html");
    expect(response.body).toContain("Link não encontrado");
  });

  test("should return 404 when campaign does not match", async () => {
    const mockEvent = {
      credentials,
      rawPath: "/:wrongcampaign/testid",
    };
    const response = await handler(mockEvent);
    expect(response.statusCode).toBe(404);
    expect(response.headers["Content-Type"]).toBe("text/html");
    expect(response.body).toContain("Link não encontrado");
  });

  test("should return 500 if table name is missing", async () => {
    const originalTable = process.env.AMAZON_DYNAMODB_TABLE;
    delete process.env.AMAZON_DYNAMODB_TABLE;

    const mockEvent = {
      credentials: {
        ...credentials,
        AMAZON_DYNAMODB_TABLE: undefined,
      },
      rawPath: "/:promodev/testid",
    };

    const response = await handler(mockEvent);
    expect(response.statusCode).toBe(500);
    expect(response.headers["Content-Type"]).toBe("text/html");
    expect(response.body).toContain("Erro no servidor");

    // Restaurando para não afetar outros testes
    process.env.AMAZON_DYNAMODB_TABLE = originalTable;
  });
});

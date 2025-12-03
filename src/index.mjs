import { getRedirectUrl } from "./controller.mjs";

const loadTemplate = async (templateName) => {
  try {
    const { promises: fs } = await import("fs");
    const { default: path } = await import("path");
    const { fileURLToPath } = await import("url");

    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);

    const templatePath = path.join(__dirname, `${templateName}.html`);
    return await fs.readFile(templatePath, "utf8");
  } catch (err) {
    console.error(`Error loading template ${templateName}:`);
    return `<html><body><h1>Erro ${templateName === "404" ? "Não Encontrado" : "Interno"}</h1></body></html>`;
  }
};

export const handler = async (event) => {
  try {
    const redirectUrl = await getRedirectUrl(event);

    if (!redirectUrl) {
      const template404 = await loadTemplate("404");
      return {
        statusCode: 404,
        headers: {
          "Content-Type": "text/html",
        },
        body: template404,
      };
    }

    return {
      statusCode: 302,
      headers: {
        Location: redirectUrl,
      },
      body: "",
    };
  } catch (error) {
    console.error("Error handling request");

    const template500 = await loadTemplate("500");
    return {
      statusCode: error.statusCode || 500,
      headers: {
        "Content-Type": "text/html",
      },
      body: template500,
    };
  }
};

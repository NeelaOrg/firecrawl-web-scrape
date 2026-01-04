import { z } from "zod";
import { config } from "../../../../config";
import { EngineScrapeResult } from "..";
import { Meta } from "../..";
import { robustFetch } from "../../lib/fetch";
import { getInnerJson } from "@mendable/firecrawl-rs";

export async function scrapeURLWithPlaywright(
  meta: Meta,
): Promise<EngineScrapeResult> {
  const response = await robustFetch({
    url: config.PLAYWRIGHT_MICROSERVICE_URL!,
    headers: {
      "Content-Type": "application/json",
    },
    body: {
      url: meta.rewrittenUrl ?? meta.url,
      wait_after_load: meta.options.waitFor,
      timeout: meta.abort.scrapeTimeout(),
      headers: meta.options.headers,
      skip_tls_verification: meta.options.skipTlsVerification,
      ...(meta.options.actions && meta.options.actions.length > 0
        ? { actions: meta.options.actions }
        : {}),
    },
    method: "POST",
    logger: meta.logger.child("scrapeURLWithPlaywright/robustFetch"),
    schema: z.object({
      content: z.string(),
      pageStatusCode: z.number(),
      pageError: z.string().optional(),
      contentType: z.string().optional(),
    }),
    mock: meta.mock,
    abort: meta.abort.asSignal(),
  });

  if (response.contentType?.includes("application/json")) {
    response.content = await getInnerJson(response.content);
  }

  return {
    url: meta.rewrittenUrl ?? meta.url, // TODO: impove redirect following
    html: response.content,
    statusCode: response.pageStatusCode,
    error: response.pageError,
    contentType: response.contentType,

    proxyUsed: "basic",
  };
}

export function playwrightMaxReasonableTime(meta: Meta): number {
  const actionWait =
    meta.options.actions?.reduce((acc, action) => {
      if (action.type === "wait") {
        return (
          acc + (action.milliseconds ?? (action.selector ? 1000 : 0))
        );
      }
      return acc + 250;
    }, 0) ?? 0;

  return (meta.options.waitFor ?? 0) + actionWait + 30000;
}

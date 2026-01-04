import express, { Request, Response } from 'express';
import bodyParser from 'body-parser';
import { chromium, Browser, BrowserContext, Route, Request as PlaywrightRequest, Page } from 'playwright';
import dotenv from 'dotenv';
import UserAgent from 'user-agents';
import { getError } from './helpers/get_error';

dotenv.config();

const app = express();
const port = process.env.PORT || 3003;

app.use(bodyParser.json());

const BLOCK_MEDIA = (process.env.BLOCK_MEDIA || 'False').toUpperCase() === 'TRUE';
const MAX_CONCURRENT_PAGES = Math.max(1, Number.parseInt(process.env.MAX_CONCURRENT_PAGES ?? '10', 10) || 10);

const PROXY_SERVER = process.env.PROXY_SERVER || null;
const PROXY_USERNAME = process.env.PROXY_USERNAME || null;
const PROXY_PASSWORD = process.env.PROXY_PASSWORD || null;
class Semaphore {
  private permits: number;
  private queue: (() => void)[] = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.queue.push(resolve);
    });
  }

  release(): void {
    this.permits++;
    if (this.queue.length > 0) {
      const nextResolve = this.queue.shift();
      if (nextResolve) {
        this.permits--;
        nextResolve();
      }
    }
  }

  getAvailablePermits(): number {
    return this.permits;
  }

  getQueueLength(): number {
    return this.queue.length;
  }
}
const pageSemaphore = new Semaphore(MAX_CONCURRENT_PAGES);

const AD_SERVING_DOMAINS = [
  'doubleclick.net',
  'adservice.google.com',
  'googlesyndication.com',
  'googletagservices.com',
  'googletagmanager.com',
  'google-analytics.com',
  'adsystem.com',
  'adservice.com',
  'adnxs.com',
  'ads-twitter.com',
  'facebook.net',
  'fbcdn.net',
  'amazon-adsystem.com'
];

type Action =
  | {
      type: 'wait';
      milliseconds?: number;
      selector?: string;
    }
  | {
      type: 'click';
      selector: string;
      all?: boolean;
    }
  | {
      type: 'scroll';
      direction?: 'up' | 'down';
      selector?: string;
    }
  | {
      type: 'write';
      text: string;
    }
  | {
      type: 'press';
      key: string;
    }
  | {
      type: 'executeJavascript';
      script: string;
    }
  | {
      type: 'screenshot';
      fullPage?: boolean;
      quality?: number;
    }
  | {
      type: 'scrape';
    }
  | {
      type: 'pdf';
      landscape?: boolean;
      scale?: number;
      format?: string;
    };

interface UrlModel {
  url: string;
  wait_after_load?: number;
  timeout?: number;
  headers?: { [key: string]: string };
  check_selector?: string;
  skip_tls_verification?: boolean;
  actions?: Action[];
}

let browser: Browser;

const initializeBrowser = async () => {
  browser = await chromium.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu'
    ]
  });
};

const createContext = async (skipTlsVerification: boolean = false) => {
  const userAgent = new UserAgent().toString();
  const viewport = { width: 1280, height: 800 };

  const contextOptions: any = {
    userAgent,
    viewport,
    ignoreHTTPSErrors: skipTlsVerification,
  };

  if (PROXY_SERVER && PROXY_USERNAME && PROXY_PASSWORD) {
    contextOptions.proxy = {
      server: PROXY_SERVER,
      username: PROXY_USERNAME,
      password: PROXY_PASSWORD,
    };
  } else if (PROXY_SERVER) {
    contextOptions.proxy = {
      server: PROXY_SERVER,
    };
  }

  const newContext = await browser.newContext(contextOptions);

  if (BLOCK_MEDIA) {
    await newContext.route('**/*.{png,jpg,jpeg,gif,svg,mp3,mp4,avi,flac,ogg,wav,webm}', async (route: Route, request: PlaywrightRequest) => {
      await route.abort();
    });
  }

  // Intercept all requests to avoid loading ads
  await newContext.route('**/*', (route: Route, request: PlaywrightRequest) => {
    const requestUrl = new URL(request.url());
    const hostname = requestUrl.hostname;

    if (AD_SERVING_DOMAINS.some(domain => hostname.includes(domain))) {
      console.log(hostname);
      return route.abort();
    }
    return route.continue();
  });
  
  return newContext;
};

const shutdownBrowser = async () => {
  if (browser) {
    await browser.close();
  }
};

const isValidUrl = (urlString: string): boolean => {
  try {
    new URL(urlString);
    return true;
  } catch (_) {
    return false;
  }
};

const scrapePage = async (
  page: Page,
  url: string,
  waitUntil: 'load' | 'networkidle',
  waitAfterLoad: number,
  timeout: number,
  checkSelector: string | undefined,
  actions: Action[] = [],
) => {
  console.log(`Navigating to ${url} with waitUntil: ${waitUntil} and timeout: ${timeout}ms`);
  const response = await page.goto(url, { waitUntil, timeout });

  if (waitAfterLoad > 0) {
    await page.waitForTimeout(waitAfterLoad);
  }

  if (checkSelector) {
    try {
      await page.waitForSelector(checkSelector, { timeout });
    } catch (error) {
      throw new Error('Required selector not found');
    }
  }

  if (actions.length > 0) {
    await runActions(page, actions, timeout);
  }

  await injectMediaLinksInDom(page, url);

  let headers = null, content = await page.content();
  let ct: string | undefined = undefined;
  if (response) {
    headers = await response.allHeaders();
    ct = Object.entries(headers).find(([key]) => key.toLowerCase() === "content-type")?.[1];
    if (ct && (ct.toLowerCase().includes("application/json") || ct.toLowerCase().includes("text/plain"))) {
      content = (await response.body()).toString("utf8"); // TODO: determine real encoding
    }
  }

  return {
    content,
    status: response ? response.status() : null,
    headers,
    contentType: ct,
  };
};

async function injectMediaLinksInDom(page: Page, baseUrl: string): Promise<void> {
  await page.evaluate(base => {
    const iframes = Array.from(
      document.querySelectorAll("iframe[src]"),
    ) as HTMLIFrameElement[];
    const headings = Array.from(
      document.querySelectorAll("h1, h2, h3, h4"),
    ) as HTMLElement[];
    const getScopedHeadings = (
      frame: HTMLIFrameElement,
      fallback: HTMLElement[],
    ): HTMLElement[] => {
      let container = frame.parentElement;
      while (container) {
        const scoped = Array.from(
          container.querySelectorAll("h1, h2, h3, h4"),
        ) as HTMLElement[];
        if (scoped.length > 0) {
          return scoped;
        }
        container = container.parentElement;
      }
      return fallback;
    };

    for (const frame of iframes) {
      if (frame.getAttribute("data-firecrawl-media-linked") === "true") {
        continue;
      }

      const src = frame.getAttribute("src");
      if (!src) {
        continue;
      }

      let resolved: string | null = null;
      try {
        resolved = new URL(src, base).href;
      } catch (_) {
        resolved = null;
      }

      if (!resolved) {
        continue;
      }

      const link = document.createElement("a");
      link.href = resolved;
      link.textContent = resolved;
      link.setAttribute("data-firecrawl-media-link", "true");

      const scopedHeadings = getScopedHeadings(frame, headings);
      const previousHeading = scopedHeadings
        .filter(
          heading =>
            (frame.compareDocumentPosition(heading) &
              Node.DOCUMENT_POSITION_PRECEDING) !==
            0,
        )
        .pop();
      const nextHeading = scopedHeadings.find(
        heading =>
          (frame.compareDocumentPosition(heading) &
            Node.DOCUMENT_POSITION_FOLLOWING) !==
          0,
      );

      if (!previousHeading && nextHeading) {
        nextHeading.insertAdjacentElement("afterend", link);
      } else {
        frame.insertAdjacentElement("afterend", link);
      }
      frame.setAttribute("data-firecrawl-media-linked", "true");
    }
  }, baseUrl);
}

async function runActions(page: Page, actions: Action[], timeout: number) {
  for (const action of actions) {
    switch (action.type) {
      case 'wait':
        if (action.milliseconds !== undefined) {
          await page.waitForTimeout(action.milliseconds);
        } else if (action.selector) {
          await page.waitForSelector(action.selector, { timeout });
        } else {
          throw new Error('wait action requires milliseconds or selector');
        }
        break;
      case 'click': {
        try {
          if (action.all) {
            await page.waitForSelector(action.selector, { timeout });
            const elements = await page.$$(action.selector);
            if (elements.length === 0) {
              throw new Error(`No elements found for selector: ${action.selector}`);
            }
            for (const element of elements) {
              await element.click();
            }
          } else {
            await page.click(action.selector, { timeout });
          }
        } catch (error) {
          if (isTimeoutError(error)) {
            console.warn(`Action click timed out, skipping selector: ${action.selector}`);
            break;
          }
          throw error;
        }
        break;
      }
      case 'scroll': {
        const direction = action.direction ?? 'down';
        if (action.selector) {
          const handle = await page.$(action.selector);
          if (!handle) {
            throw new Error(`No element found for selector: ${action.selector}`);
          }
          await handle.evaluate(
            (el, dir) => {
              if (dir === 'down') {
                (el as HTMLElement).scrollTop = (el as HTMLElement).scrollHeight;
              } else {
                (el as HTMLElement).scrollTop = 0;
              }
            },
            direction,
          );
        } else {
          await page.evaluate(dir => {
            if (dir === 'down') {
              window.scrollTo(0, document.body.scrollHeight);
            } else {
              window.scrollTo(0, 0);
            }
          }, direction);
        }
        break;
      }
      case 'write':
        await page.keyboard.type(action.text);
        break;
      case 'press':
        await page.keyboard.press(action.key);
        break;
      case 'executeJavascript':
        await page.evaluate(script => (0, eval)(script), action.script);
        break;
      case 'screenshot':
      case 'scrape':
      case 'pdf':
        throw new Error(`Unsupported action type: ${action.type}`);
      default:
        assertNever(action);
    }
  }
}

function isTimeoutError(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }
  const candidate = error as { name?: string; message?: string };
  return candidate.name === "TimeoutError" || candidate.message?.includes("Timeout") === true;
}

const assertNever = (value: never): never => {
  throw new Error(`Unsupported action type: ${JSON.stringify(value)}`);
};

app.get('/health', async (req: Request, res: Response) => {
  try {
    if (!browser) {
      await initializeBrowser();
    }
    
    const testContext = await createContext();
    const testPage = await testContext.newPage();
    await testPage.close();
    await testContext.close();
    
    res.status(200).json({ 
      status: 'healthy',
      maxConcurrentPages: MAX_CONCURRENT_PAGES,
      activePages: MAX_CONCURRENT_PAGES - pageSemaphore.getAvailablePermits()
    });
  } catch (error) {
    console.error('Health check failed:', error);
    res.status(503).json({ 
      status: 'unhealthy', 
      error: error instanceof Error ? error.message : 'Unknown error occurred'
    });
  }
});

app.post('/scrape', async (req: Request, res: Response) => {
  const {
    url,
    wait_after_load = 0,
    timeout = 15000,
    headers,
    check_selector,
    skip_tls_verification = false,
    actions = [],
  }: UrlModel = req.body;

  console.log(`================= Scrape Request =================`);
  console.log(`URL: ${url}`);
  console.log(`Wait After Load: ${wait_after_load}`);
  console.log(`Timeout: ${timeout}`);
  console.log(`Headers: ${headers ? JSON.stringify(headers) : 'None'}`);
  console.log(`Check Selector: ${check_selector ? check_selector : 'None'}`);
  console.log(`Skip TLS Verification: ${skip_tls_verification}`);
  console.log(`Actions: ${actions.length}`);
  console.log(`==================================================`);

  if (!url) {
    return res.status(400).json({ error: 'URL is required' });
  }

  if (!isValidUrl(url)) {
    return res.status(400).json({ error: 'Invalid URL' });
  }

  if (!PROXY_SERVER) {
    console.warn('⚠️ WARNING: No proxy server provided. Your IP address may be blocked.');
  }

  if (!browser) {
    await initializeBrowser();
  }

  await pageSemaphore.acquire();
  
  let requestContext: BrowserContext | null = null;
  let page: Page | null = null;

  try {
    requestContext = await createContext(skip_tls_verification);
    page = await requestContext.newPage();

    if (headers) {
      await page.setExtraHTTPHeaders(headers);
    }

    const result = await scrapePage(
      page,
      url,
      'load',
      wait_after_load,
      timeout,
      check_selector,
      actions,
    );
    const pageError = result.status !== 200 ? getError(result.status) : undefined;

    if (!pageError) {
      console.log(`✅ Scrape successful!`);
    } else {
      console.log(`🚨 Scrape failed with status code: ${result.status} ${pageError}`);
    }

    res.json({
      content: result.content,
      pageStatusCode: result.status,
      contentType: result.contentType,
      ...(pageError && { pageError })
    });

  } catch (error) {
    console.error('Scrape error:', error);
    res.status(500).json({ error: 'An error occurred while fetching the page.' });
  } finally {
    if (page) await page.close();
    if (requestContext) await requestContext.close();
    pageSemaphore.release();
  }
});

app.listen(port, () => {
  initializeBrowser().then(() => {
    console.log(`Server is running on port ${port}`);
  });
});

if (require.main === module) {
  process.on('SIGINT', () => {
    shutdownBrowser().then(() => {
      console.log('Browser closed');
      process.exit(0);
    });
  });
}

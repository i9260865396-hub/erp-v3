export const API_URL =
  process.env.NEXT_PUBLIC_API_URL?.replace(/\/$/, "") || "http://localhost:8000";

async function parseJsonSafe(res: Response) {
  const txt = await res.text();
  try {
    return txt ? JSON.parse(txt) : null;
  } catch {
    return txt;
  }
}

function normalizeBody(body: any): BodyInit | undefined {
  if (body == null) return undefined;
  if (typeof body === "string") return body;
  // fetch BodyInit types
  if (body instanceof Blob) return body;
  // FormData / URLSearchParams / ArrayBuffer etc
  if (typeof FormData !== "undefined" && body instanceof FormData) return body;
  if (typeof URLSearchParams !== "undefined" && body instanceof URLSearchParams) return body;
  // default JSON
  return JSON.stringify(body);
}

export async function api<T>(path: string, opts?: RequestInit): Promise<T> {
  const normalized = normalizeBody((opts as any)?.body);
  const isForm = typeof FormData !== "undefined" && normalized instanceof FormData;

  const headers: Record<string, string> = { ...(opts?.headers as any) };
  // For JSON bodies we set Content-Type explicitly. For FormData, browser sets boundary itself.
  if (!isForm) {
    if (!headers["Content-Type"]) headers["Content-Type"] = "application/json";
  } else {
    // Ensure we don't override boundary
    if (headers["Content-Type"]) delete headers["Content-Type"];
  }

  const res = await fetch(`${API_URL}${path}`, {
    ...opts,
    body: normalized,
    headers,
    cache: "no-store",
  });

  if (!res.ok) {
    const bodyErr = await parseJsonSafe(res);
    const msg =
      (bodyErr && typeof bodyErr === "object" && "detail" in bodyErr
        ? (bodyErr as any).detail
        : JSON.stringify(bodyErr)) || `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }

  return (await res.json()) as T;
}

"use client";

import type { Atom, ExtractAtomValue, PrimitiveAtom } from "jotai";
import { atom, createStore, useAtomValue, useSetAtom } from "jotai";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

// ── Context ──────────────────────────────────────────────────────────────────

interface ObjectsReactContextType {
  /** Base URL of the Beyond objects server (e.g. `https://objects.beyond.dev`). */
  url: string;
  /** Bucket to upload into. */
  bucket: string;
  /**
   * Called once per upload. Returns a short-lived Bearer token scoped to the
   * given key. Implement this by calling your own server-side endpoint, which
   * calls `client.createUploadToken(key)` from `@beyond.dev/objects`.
   */
  generateToken: (file: File, key: string) => Promise<{ token: string }>;
  /** Maximum file size in bytes. Files that exceed this are rejected client-side. */
  maxUploadSize?: number;
}

const ObjectsReactContext = createContext<ObjectsReactContextType>({
  url: "",
  bucket: "default",
  generateToken: async () => ({ token: "" }),
});

/** A Jotai store used exclusively by these components (isolated from app state). */
const filesStore = createStore();

// ── Provider ─────────────────────────────────────────────────────────────────

/**
 * Provides configuration to all Beyond upload hooks and components.
 * Place this near the root of your upload UI.
 */
export function ObjectsProvider({
  children,
  ...props
}: ObjectsReactContextType & { children: React.ReactNode }) {
  return (
    <ObjectsReactContext.Provider value={props}>
      {children}
    </ObjectsReactContext.Provider>
  );
}

// ── Constants ─────────────────────────────────────────────────────────────────

/** Convenience MIME type string for image-only file inputs. */
export const IMAGE_MIMES = "image/*";

// ── File selection hooks ──────────────────────────────────────────────────────

/**
 * Returns a callback that opens the browser file picker dialog.
 * The `onSelect` callback receives a `SelectedFile` atom for each chosen file.
 */
export function useSelectFiles(
  options: {
    /** Comma-separated list of accepted MIME types or file extensions. */
    accept?: string;
    /** Allow selecting multiple files. Default `true`. */
    multiple?: boolean;
    /** Called for each selected file. */
    onSelect?: OnSelect;
  } = {},
): SelectFilesCallback {
  const storedOptions = useRef(options);
  useEffect(() => {
    storedOptions.current = options;
  });

  return useCallback(function selectFiles({ key } = {}) {
    const el = document.createElement("input");
    el.type = "file";
    el.multiple = storedOptions.current.multiple ?? true;
    if (storedOptions.current.accept) {
      el.accept = storedOptions.current.accept;
    }

    const onChange: EventListener = async (e) => {
      if (e.target instanceof HTMLInputElement) {
        for (const file of e.target.files ?? []) {
          storedOptions.current.onSelect?.(await createFile(file, key));
        }
        el.removeEventListener("change", onChange);
        el.remove();
      }
    };

    el.addEventListener("change", onChange);
    el.click();
  }, []);
}

/**
 * Returns a callback that opens the browser directory picker dialog.
 * Each file within the selected directory is passed to `onSelect`.
 */
export function useSelectDirectory(
  options: {
    /** Called for each file in the selected directory. */
    onSelect?: OnSelect;
  } = {},
): SelectDirectoryCallback {
  const storedOptions = useRef(options);
  useEffect(() => {
    storedOptions.current = options;
  });

  return useCallback(function selectDirectory({ key } = {}) {
    const el = document.createElement("input");
    el.type = "file";
    el.webkitdirectory = true;

    const onChange: EventListener = async (e) => {
      if (e.target instanceof HTMLInputElement) {
        for (const file of e.target.files ?? []) {
          storedOptions.current.onSelect?.(await createFile(file, key));
        }
        el.removeEventListener("change", onChange);
        el.remove();
      }
    };

    el.addEventListener("change", onChange);
    el.click();
  }, []);
}

/**
 * Attaches drag-and-drop event handlers to any element.
 * Spread `props` onto the drop target element.
 */
export function useDropFiles(
  options: {
    /** Key used for dropped files. Can be a string or a function of `File`. */
    key?: Key;
    /** Comma-separated list of accepted MIME types or extensions. */
    accept?: string;
    /** Allow dropping multiple files at once. Default `true`. */
    multiple?: boolean;
    /** Called for each accepted dropped file. */
    onSelect?: OnSelect;
  } = {},
): {
  props: React.HTMLAttributes<HTMLElement>;
  /** `true` while the user is dragging a file over the drop target. */
  isActive: boolean;
} {
  const [isActive, setIsActive] = useState(false);
  const storedOptions = useRef(options);
  useEffect(() => {
    storedOptions.current = options;
  });

  const props = useMemo<React.HTMLAttributes<HTMLElement>>(
    () => ({
      onDragEnter(e) {
        e.preventDefault();
        setIsActive(true);
      },
      onDragOver(e) {
        e.preventDefault();
        setIsActive(true);
      },
      onDragLeave(e) {
        e.preventDefault();
        setIsActive(false);
      },
      async onDrop(e) {
        e.preventDefault();
        const key = storedOptions.current.key;
        const accepts = storedOptions.current.accept
          ?.split(",")
          .map((s) => s.trim());

        for (const file of e.dataTransfer.files) {
          let accepted = !accepts?.length;
          if (file.type && accepts?.length) {
            for (const accept of accepts) {
              if (
                file.type === accept
                || (accept.includes("/*")
                  && file.type.startsWith(accept.replace("/*", "/")))
                || (accept.startsWith(".")
                  && file.name.toLowerCase().endsWith(accept.toLowerCase()))
              ) {
                accepted = true;
                break;
              }
            }
          }

          if (!accepted) continue;
          storedOptions.current.onSelect?.(await createFile(file, key));
          if (!storedOptions.current.multiple) break;
        }

        setIsActive(false);
      },
      "data-dropzone-active": isActive,
    }),
    [isActive],
  );

  return { props, isActive };
}

/**
 * Combines `useSelectFiles` and `useDropFiles` into a single clickable dropzone.
 * Spread `props` onto any element to make it both clickable and a drop target.
 */
export function useDropzone(options: {
  accept?: string;
  multiple?: boolean;
  key?: Key;
  onSelect?: OnSelect;
}) {
  const selectFiles = useSelectFiles(options);
  const dropFiles = useDropFiles(options);
  const props = useMemo<React.HTMLAttributes<HTMLElement>>(
    () => ({
      ...dropFiles.props,
      onClick(e) {
        e.preventDefault();
        selectFiles(
          options.key !== undefined ? { key: options.key } : undefined,
        );
      },
    }),
    [dropFiles.props, selectFiles, options.key],
  );

  return { props, isActive: dropFiles.isActive };
}

// ── File atom creation ────────────────────────────────────────────────────────

/**
 * Create a `SelectedFile` atom programmatically — without a browser file
 * picker dialog. Useful for testing and for flows where you already have a
 * `File` reference (e.g. from a paste event or a camera capture).
 */
export async function createFile(file: File, key?: Key): Promise<SelectedFile> {
  const k = await Promise.resolve(
    typeof key === "function" ? key(file) : key,
  );
  const data = {
    key: `${(k ?? (file.webkitRelativePath || file.name)).replace(/^\//, "")}`,
    file,
  };

  const bytesUploaded = atom(0);
  return atom<SelectedFileData>({
    ...data,
    bytesUploaded,
    progress: atom((get) => get(bytesUploaded) / file.size),
    startTime: atom<number | null>(null),
    progressSamples: atom<Array<ProgressSample>>([]),
    status: atom<ExtractAtomValue<SelectedFileData["status"]>>("idle"),
    abortController: new AbortController(),
  });
}

// ── Status & progress hooks ───────────────────────────────────────────────────

/** Returns the current upload status of a selected file. */
export function useStatus(
  selectedFile: SelectedFile,
): ExtractAtomValue<SelectedFileData["status"]> {
  return useAtomValue(
    useAtomValue(selectedFile, { store: filesStore }).status,
    { store: filesStore },
  );
}

function calculateProgress(
  loaded: number,
  total: number,
  startTime: number | null,
  samples: Array<ProgressSample> = [],
): ProgressData {
  const now = Date.now();
  const timeElapsed = now - (startTime ?? now);
  let rate = 0;
  if (samples.length > 1) {
    const oldest = samples[0]!;
    const timeDiff = (now - oldest.time) / 1000;
    const bytesDiff = loaded - oldest.loaded;
    rate = bytesDiff / timeDiff;
  }
  const progress = loaded / total;
  return {
    loaded,
    total,
    progress,
    rate,
    estimatedTimeRemaining: progress === 1
      ? 0
      : rate > 0
      ? ((total - loaded) / rate) * 1000
      : null,
    timeElapsed,
  };
}

/**
 * Returns detailed upload progress for a file: speed, ETA, bytes uploaded.
 */
export function useProgress(selectedFile: SelectedFile): ProgressData {
  const file = useAtomValue(selectedFile, { store: filesStore });
  const bytesUploaded = useAtomValue(file.bytesUploaded, { store: filesStore });
  const startTime = useAtomValue(file.startTime, { store: filesStore });
  const samples = useAtomValue(file.progressSamples, { store: filesStore });
  return useMemo(
    () => calculateProgress(bytesUploaded, file.file.size, startTime, samples),
    [bytesUploaded, file.file.size, startTime, samples],
  );
}

/** Returns the raw `File` object and key from a selected file atom. */
export function useSelectedFile(
  selectedFile: SelectedFile,
): Pick<SelectedFileData, "key" | "file"> {
  const file = useAtomValue(selectedFile, { store: filesStore });
  return useMemo(() => ({ key: file.key, file: file.file }), [
    file.file,
    file.key,
  ]);
}

/** Returns a callback that aborts an in-progress upload. */
export function useAbort(selectedFile: SelectedFile): () => void {
  const file = useAtomValue(selectedFile, { store: filesStore });
  const setStatus = useSetAtom(file.status, { store: filesStore });
  return useCallback(() => {
    const status = filesStore.get(file.status);
    if (status !== "success" && status !== "error") {
      file.abortController.abort();
      setStatus("aborted");
    }
  }, [setStatus, file.status, file.abortController]);
}

// ── Upload hooks ──────────────────────────────────────────────────────────────

/**
 * Returns a callback that uploads a single file directly to the Beyond objects
 * server. Calls `generateToken` from the provider to obtain a short-lived
 * Bearer token before each upload, so your server remains the auth gate.
 */
export function useUploadFile() {
  const ctx = useContext(ObjectsReactContext);

  return useCallback(
    async function uploadFile(
      file: SelectedFile,
      options: UploadFileOptions = {},
    ) {
      const { onProgress, onAbort, onSuccess, onError } = options;
      const { get, set } = filesStore;
      const f = get(file);
      const key = await Promise.resolve(
        typeof options.key === "function"
          ? options.key(f.file)
          : options.key ?? f.key,
      );

      if (ctx.maxUploadSize && f.file.size > ctx.maxUploadSize) {
        set(f.status, "error");
        const error = new Error(
          `File is too large. Max size is ${ctx.maxUploadSize} bytes.`,
        );
        set(file, (current) => ({ ...current, error: error.message }));
        onError?.({ key, file: f.file }, error);
        return;
      }

      const uploadingFile = get(file);
      if (get(uploadingFile.status) === "aborted") return;

      set(uploadingFile.status, "uploading");

      const abortSignal = f.abortController.signal;
      const handleAbortSignal = (): void => {
        onAbort?.({ key, file: f.file });
        set(uploadingFile.status, "aborted");
        abortSignal.removeEventListener("abort", handleAbortSignal);
      };
      abortSignal.addEventListener("abort", handleAbortSignal);

      if (
        f.abortController.signal.aborted
        || get(uploadingFile.status) === "aborted"
      ) {
        return;
      }

      // Obtain a short-lived token from the user's server before each upload.
      let token: string;
      try {
        const result = await ctx.generateToken(f.file, key);
        token = result.token;
      } catch (err) {
        set(uploadingFile.status, "error");
        const message = err instanceof Error
          ? err.message
          : "Failed to generate upload token";
        set(file, (current) => ({ ...current, error: message }));
        onError?.({ key, file: f.file }, err);
        abortSignal.removeEventListener("abort", handleAbortSignal);
        return;
      }

      const uploadUrl = joinPath(ctx.url, "v1", ctx.bucket, key as string);
      let response: Response;

      try {
        response = await new Promise((resolve, reject) => {
          const xhr = new XMLHttpRequest();

          abortSignal.addEventListener("abort", () => {
            xhr.abort();
            reject(new DOMException("Aborted", "AbortError"));
          });

          set(f.startTime, Date.now());
          set(uploadingFile.progressSamples, []);

          xhr.upload.addEventListener("progress", (e) => {
            if (e.lengthComputable) {
              set(f.bytesUploaded, e.loaded);
              set(
                f.progressSamples,
                (current) =>
                  [...current, { time: Date.now(), loaded: e.loaded }].slice(
                    -10,
                  ),
              );
              onProgress?.(
                { key, file: f.file },
                calculateProgress(
                  e.loaded,
                  e.total,
                  get(f.startTime),
                  get(f.progressSamples),
                ),
              );
            }
          });

          xhr.addEventListener("load", () => {
            resolve(
              new Response(xhr.response, {
                status: xhr.status,
                statusText: xhr.statusText,
                headers: parseHeaders(xhr.getAllResponseHeaders()),
              }),
            );
          });
          xhr.addEventListener(
            "error",
            () => reject(new Error("Upload failed")),
          );

          // open() must be called before setRequestHeader()
          xhr.open("PUT", uploadUrl);
          xhr.setRequestHeader("Authorization", `Bearer ${token}`);
          for (const name in options.headers ?? {}) {
            xhr.setRequestHeader(name, options.headers![name]!);
          }
          xhr.send(f.file);
        });

        if (!response.ok) {
          try {
            throw await response.text();
          } catch (e) {
            throw `${response.status}: ${response.statusText}`;
          }
        }
      } catch (err) {
        if (err instanceof DOMException && err.name === "AbortError") return;
        set(uploadingFile.status, "error");
        const error = typeof err === "string"
          ? err
          : err instanceof Error
          ? err.message
          : "An unknown error occurred";
        set(file, (current) => ({ ...current, error }));
        onError?.({ key, file: f.file }, err);
      } finally {
        abortSignal.removeEventListener("abort", handleAbortSignal);
      }

      if (get(uploadingFile.status) === "uploading") {
        set(f.bytesUploaded, f.file.size);
        set(uploadingFile.status, "success");
        set(file, (current) => ({ ...current, response }));
        onSuccess?.({ key, file: f.file }, response!);
      }
    },
    [ctx.maxUploadSize, ctx.url, ctx.bucket, ctx.generateToken],
  );
}

/**
 * Returns a callback that uploads multiple files concurrently with a
 * configurable concurrency limit (default 3).
 */
export function useUploadFiles() {
  const upload = useUploadFile();

  return useCallback(
    async function uploadFiles(
      selectedFiles: Array<SelectedFile>,
      options: { concurrency?: number } & UploadFileOptions = {},
    ) {
      const concurrency = options.concurrency ?? 3;
      const chunks = selectedFiles.reduce<Array<SelectedFile[]>>(
        (acc, file, i) => {
          const chunkIndex = Math.floor(i / concurrency);
          if (!acc[chunkIndex]) acc[chunkIndex] = [];
          const f = filesStore.get(file);
          const status = filesStore.get(f.status);
          if (status === "aborted" || f.abortController.signal.aborted) {
            return acc;
          }
          filesStore.set(f.status, "queued");
          acc[chunkIndex].push(file);
          return acc;
        },
        [],
      );

      for (const chunk of chunks) {
        await Promise.all(chunk.map((file) => upload(file, options)));
      }
    },
    [upload],
  );
}

// ── Preview hook ──────────────────────────────────────────────────────────────

/**
 * Generates a local preview URL for an image file (data: URI). Automatically
 * revokes the URL when the component unmounts.
 */
export function usePreview(file: SelectedFile) {
  const [state, setState] = useState<PreviewState>(initialPreviewState);
  const clearPreview = useCallback(() => setState(initialPreviewState), []);

  useEffect(() => {
    const f = filesStore.get(file);
    if (!f.file) {
      clearPreview();
      return;
    }

    setState({ status: "loading", error: null, data: null });

    if (!f.file.type.startsWith("image/")) {
      setState({
        data: null,
        error: "Selected file is not an image",
        status: "error",
      });
      return;
    }

    const reader = new FileReader();
    reader.onload = (e) => {
      if (
        e.target instanceof FileReader && typeof e.target.result === "string"
      ) {
        setState({ data: e.target.result, error: null, status: "success" });
      }
    };
    reader.onerror = () =>
      setState({ data: null, error: "Error reading file", status: "error" });
    reader.readAsDataURL(f.file);

    return () => {
      reader.abort();
      clearPreview();
    };
  }, [file, clearPreview]);

  useEffect(() => {
    return () => {
      if (state.data?.startsWith("blob:")) URL.revokeObjectURL(state.data);
    };
  }, [state.data]);

  return state;
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/**
 * Returns the SHA-256 hash of a file as a lowercase hex string.
 * Useful for generating deterministic object keys.
 */
export async function hashFile(file: File): Promise<string> {
  const buffer = await file.arrayBuffer();
  const hashBuffer = await crypto.subtle.digest("SHA-256", buffer);
  return Array.from(new Uint8Array(hashBuffer))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

/**
 * Returns the lowercase file extension including the dot (e.g. `".png"`),
 * or `""` for dotfiles and files with no extension.
 */
export function extname(file: File): string {
  if (!file?.name) return "";
  const name = file.name.trim();
  const lastDot = name.lastIndexOf(".");
  if (lastDot <= 0 || lastDot === name.length - 1) return "";
  return name.slice(lastDot).toLowerCase();
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function joinPath(...parts: string[]): string {
  const [base, ...rest] = parts;
  if (!base) return rest.join("/");
  try {
    const u = new URL(base);
    u.pathname = ["", ...rest].join("/").replace(/\/{2,}/g, "/");
    return u.toString();
  } catch {
    return parts.join("/").replace(/\/{2,}/g, "/");
  }
}

function parseHeaders(headerStr: string): Headers {
  const headers = new Headers();
  if (!headerStr) return headers;
  for (const line of headerStr.trim().split(/[\r\n]+/)) {
    const parts = line.split(": ");
    const key = parts.shift();
    const value = parts.join(": ");
    if (key && value) headers.append(key.trim(), value.trim());
  }
  return headers;
}

const initialPreviewState: PreviewState = {
  data: null,
  error: null,
  status: "idle",
};

type PreviewState =
  | { data: null; error: null; status: "idle" | "loading" }
  | { data: string; error: null; status: "success" }
  | { data: null; error: string; status: "error" };

// ── Public types ──────────────────────────────────────────────────────────────

export type ProgressData = {
  loaded: number;
  total: number;
  progress: number;
  rate: number;
  estimatedTimeRemaining: number | null;
  timeElapsed: number;
};

export type SelectedFileData = {
  key: string;
  file: File;
  bytesUploaded: PrimitiveAtom<number>;
  progress: Atom<number>;
  status: PrimitiveAtom<SelectedFileStatus>;
  startTime: PrimitiveAtom<number | null>;
  progressSamples: PrimitiveAtom<Array<ProgressSample>>;
  error?: string;
  abortController: AbortController;
};

export type SelectedFile = PrimitiveAtom<SelectedFileData>;
export type SelectedFileStatus =
  | "idle"
  | "queued"
  | "uploading"
  | "aborted"
  | "success"
  | "error";
export type OnSelect = (file: SelectedFile) => void | Promise<void>;
export type Key = string | ((file: File) => string | Promise<string>);
export type SelectFilesCallback = (options?: { key?: Key }) => void;
export type SelectDirectoryCallback = SelectFilesCallback;
export type ProgressSample = { time: number; loaded: number };

type UploadFileOptions = {
  key?: Key;
  headers?: Record<string, string>;
  onAbort?: (selectedFileData: Pick<SelectedFileData, "key" | "file">) => void;
  onSuccess?: (
    selectedFileData: Pick<SelectedFileData, "key" | "file">,
    response: Response,
  ) => Promise<void> | void;
  onProgress?: (
    selectedFileData: Pick<SelectedFileData, "key" | "file">,
    progress: ProgressData,
  ) => Promise<void> | void;
  onError?: (
    selectedFileData: Pick<SelectedFileData, "key" | "file">,
    err: unknown,
  ) => Promise<void> | void;
};

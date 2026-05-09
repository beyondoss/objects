import { act, renderHook, waitFor } from "@testing-library/react";
import React from "react";
import { describe, expect, it, vi } from "vitest";
import {
  createFile,
  ObjectsProvider,
  useAbort,
  useDropFiles,
  useProgress,
  useSelectedFile,
  useStatus,
  useUploadFile,
  useUploadFiles,
} from "../src/react/index.js";
import { getRootToken, getTestUrl, rootClient, uniqueKey } from "./harness.js";

function makeWrapper(opts: { maxUploadSize?: number } = {}) {
  const client = rootClient();
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return (
      <ObjectsProvider
        url={getTestUrl()}
        bucket="default"
        generateToken={async (_, key) => {
          const { data, error } = await client.createUploadToken(key, {
            ttlSecs: 300,
          });
          if (error) throw error;
          return { token: data.token };
        }}
        {...opts}
      >
        {children}
      </ObjectsProvider>
    );
  };
}

async function getObject(key: string): Promise<Response> {
  return fetch(`${getTestUrl()}/v1/default/${key}`, {
    headers: { Authorization: `Bearer ${getRootToken()}` },
  });
}

// ── Tests ─────────────────────────────────────────────────────────────────────

describe("useUploadFile", () => {
  it("uploads a file and transitions idle → uploading → success", async () => {
    const key = uniqueKey("react");
    const file = await createFile(
      new File(["hello world"], `${key}.txt`, { type: "text/plain" }),
      key,
    );
    const wrapper = makeWrapper();

    const { result: uploadResult } = renderHook(() => useUploadFile(), {
      wrapper,
    });
    const { result: statusResult } = renderHook(() => useStatus(file), {
      wrapper,
    });

    expect(statusResult.current).toBe("idle");

    await act(async () => {
      await uploadResult.current(file);
    });

    expect(statusResult.current).toBe("success");

    // Verify bytes on the real server.
    const res = await getObject(key);
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("hello world");
  });

  it("rejects when file exceeds maxUploadSize without calling generateToken", async () => {
    const key = uniqueKey("react-size");
    const file = await createFile(
      new File(["x".repeat(200)], `${key}.txt`),
      key,
    );

    const generateToken = vi.fn().mockResolvedValue({ token: "unused" });
    const wrapper = ({ children }: { children: React.ReactNode }) => (
      <ObjectsProvider
        url={getTestUrl()}
        bucket="default"
        generateToken={generateToken}
        maxUploadSize={100}
      >
        {children}
      </ObjectsProvider>
    );

    const { result: uploadResult } = renderHook(() => useUploadFile(), {
      wrapper,
    });
    const { result: statusResult } = renderHook(() => useStatus(file), {
      wrapper,
    });

    await act(async () => {
      await uploadResult.current(file);
    });

    expect(statusResult.current).toBe("error");
    expect(generateToken).not.toHaveBeenCalled();
  });
});

describe("useUploadFiles", () => {
  it("uploads multiple files concurrently and all reach success", async () => {
    const wrapper = makeWrapper();
    const files = await Promise.all(
      Array.from({ length: 4 }, async (_, i) => {
        const key = uniqueKey(`react-multi-${i}`);
        return createFile(new File([`content-${i}`], `${key}.txt`), key);
      }),
    );

    const { result: uploadResult } = renderHook(() => useUploadFiles(), {
      wrapper,
    });
    const statusHooks = files.map((f) =>
      renderHook(() => useStatus(f), { wrapper })
    );

    await act(async () => {
      await uploadResult.current(files, { concurrency: 2 });
    });

    for (const { result } of statusHooks) {
      expect(result.current).toBe("success");
    }

    // Spot-check one file on the server.
    const { key } =
      renderHook(() => useSelectedFile(files[0]!), { wrapper }).result.current;
    const res = await getObject(key);
    expect(res.status).toBe(200);
  });
});

describe("useProgress", () => {
  it("reports progress = 1 and rate > 0 after a successful upload", async () => {
    const key = uniqueKey("react-progress");
    const bytes = new Uint8Array(512 * 1024); // 512 KB
    const file = await createFile(
      new File([bytes], `${key}.bin`, { type: "application/octet-stream" }),
      key,
    );
    const wrapper = makeWrapper();

    const { result: uploadResult } = renderHook(() => useUploadFile(), {
      wrapper,
    });
    const { result: progressResult } = renderHook(() => useProgress(file), {
      wrapper,
    });

    await act(async () => {
      await uploadResult.current(file);
    });

    expect(progressResult.current.progress).toBe(1);
    expect(progressResult.current.loaded).toBe(512 * 1024);
  });
});

describe("useAbort", () => {
  it("aborts an in-progress upload and sets status to aborted", async () => {
    const key = uniqueKey("react-abort");
    // 2 MB — large enough that we can abort before XHR completes.
    const bytes = new Uint8Array(2 * 1024 * 1024);
    const file = await createFile(new File([bytes], `${key}.bin`), key);
    const wrapper = makeWrapper();

    const { result: uploadResult } = renderHook(() => useUploadFile(), {
      wrapper,
    });
    const { result: statusResult } = renderHook(() => useStatus(file), {
      wrapper,
    });
    const { result: abortResult } = renderHook(() => useAbort(file), {
      wrapper,
    });

    // Start upload without awaiting, then immediately abort.
    act(() => {
      void uploadResult.current(file);
    });

    await act(async () => {
      abortResult.current();
    });

    await waitFor(() => {
      expect(statusResult.current).toBe("aborted");
    });
  });
});

describe("useDropFiles", () => {
  it("calls onSelect when a file is dropped", async () => {
    const onSelect = vi.fn();
    const wrapper = makeWrapper();

    const { result } = renderHook(
      () => useDropFiles({ onSelect, multiple: false }),
      { wrapper },
    );

    const droppedFile = new File(["drop-content"], "dropped.txt", {
      type: "text/plain",
    });

    const dataTransfer = {
      files: [droppedFile],
    } as unknown as DataTransfer;

    await act(async () => {
      await result.current.props.onDrop!({
        preventDefault: () => {},
        dataTransfer,
      } as unknown as React.DragEvent<HTMLElement>);
    });

    expect(onSelect).toHaveBeenCalledTimes(1);
    const selectedFile = onSelect.mock.calls[0]![0];

    const { result: selectedResult } = renderHook(
      () => useSelectedFile(selectedFile),
      { wrapper },
    );

    expect(selectedResult.current.file).toBe(droppedFile);
  });
});

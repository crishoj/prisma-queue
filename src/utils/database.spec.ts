import { beforeEach, describe, expect, it, vi } from "vitest";
import { detectDatabaseProvider, databaseProvider, resetDatabaseProviderCache } from "./database";

describe("detectDatabaseProvider", () => {
  beforeEach(() => {
    resetDatabaseProviderCache();
  });

  it("should detect PostgreSQL", async () => {
    const mockPrisma = {
      $queryRaw: vi.fn()
        .mockResolvedValueOnce([]) // First SELECT 1 succeeds
        .mockResolvedValueOnce([{ server_version: "14.0" }]), // SHOW server_version succeeds
    };

    const provider = await detectDatabaseProvider(mockPrisma);
    expect(provider).toBe("postgresql");
    expect(mockPrisma.$queryRaw).toHaveBeenCalledTimes(2);
  });

  it("should detect SQLite", async () => {
    const mockPrisma = {
      $queryRaw: vi.fn()
        .mockRejectedValueOnce(new Error("PostgreSQL query failed")) // First SELECT 1 fails
        .mockResolvedValueOnce([{ "sqlite_version()": "3.39.0" }]), // SQLite query succeeds
    };

    const provider = await detectDatabaseProvider(mockPrisma);
    expect(provider).toBe("sqlite");
    expect(mockPrisma.$queryRaw).toHaveBeenCalledTimes(2);
  });

  it("should detect MySQL", async () => {
    const mockPrisma = {
      $queryRaw: vi.fn()
        .mockRejectedValueOnce(new Error("PostgreSQL query failed")) // PostgreSQL fails
        .mockRejectedValueOnce(new Error("SQLite query failed")) // SQLite fails
        .mockResolvedValueOnce([{ "VERSION()": "8.0.32" }]), // MySQL succeeds
    };

    const provider = await detectDatabaseProvider(mockPrisma);
    expect(provider).toBe("mysql");
    expect(mockPrisma.$queryRaw).toHaveBeenCalledTimes(3);
  });

  it("should default to PostgreSQL when all detection fails", async () => {
    const mockPrisma = {
      $queryRaw: vi.fn()
        .mockRejectedValue(new Error("All queries failed")),
    };

    const provider = await detectDatabaseProvider(mockPrisma);
    expect(provider).toBe("postgresql");
    expect(mockPrisma.$queryRaw).toHaveBeenCalledTimes(4);
  });

  it("should not cache results (no caching)", async () => {
    const mockPrisma = {
      $queryRaw: vi.fn()
        .mockResolvedValueOnce([]) // First SELECT 1 succeeds
        .mockResolvedValueOnce([{ server_version: "14.0" }]) // SHOW server_version succeeds
        .mockResolvedValueOnce([]) // Second call - SELECT 1 succeeds
        .mockResolvedValueOnce([{ server_version: "14.0" }]), // Second call - SHOW server_version succeeds
    };

    const provider1 = await detectDatabaseProvider(mockPrisma);
    const provider2 = await detectDatabaseProvider(mockPrisma);
    
    expect(provider1).toBe("postgresql");
    expect(provider2).toBe("postgresql");
    expect(mockPrisma.$queryRaw).toHaveBeenCalledTimes(4); // Should call again since no caching
  });
});

describe("databaseProvider", () => {
  beforeEach(() => {
    resetDatabaseProviderCache();
  });

  it("should detect PostgreSQL with caching", async () => {
    const mockPrisma = {
      $queryRaw: vi.fn()
        .mockResolvedValueOnce([]) // First SELECT 1 succeeds
        .mockResolvedValueOnce([{ server_version: "14.0" }]), // SHOW server_version succeeds
    };

    const provider = await databaseProvider(mockPrisma);
    expect(provider).toBe("postgresql");
    expect(mockPrisma.$queryRaw).toHaveBeenCalledTimes(2);
  });

  it("should use cached result on subsequent calls", async () => {
    const mockPrisma = {
      $queryRaw: vi.fn()
        .mockResolvedValueOnce([]) // First SELECT 1 succeeds
        .mockResolvedValueOnce([{ server_version: "14.0" }]), // SHOW server_version succeeds
    };

    const provider1 = await databaseProvider(mockPrisma);
    const provider2 = await databaseProvider(mockPrisma);
    
    expect(provider1).toBe("postgresql");
    expect(provider2).toBe("postgresql");
    expect(mockPrisma.$queryRaw).toHaveBeenCalledTimes(2); // Should not call again due to caching
  });
});
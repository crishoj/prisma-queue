import { debug } from "src/utils/debug";

export type DatabaseProvider = "postgresql" | "sqlite" | "mysql" | "sqlserver" | "mongodb" | "cockroachdb";

export interface PrismaLike {
  $queryRaw: (query: any, ...values: any[]) => Promise<any>;
}

let cachedProvider: DatabaseProvider | null = null;

export function resetDatabaseProviderCache(): void {
  cachedProvider = null;
}

export async function detectDatabaseProvider(prisma: PrismaLike): Promise<DatabaseProvider> {
  try {
    await prisma.$queryRaw`SELECT 1`;
    await prisma.$queryRaw`SHOW server_version`;
    return "postgresql";
  } catch {
    try {
      await prisma.$queryRaw`SELECT sqlite_version()`;
      return "sqlite";
    } catch {
      try {
        await prisma.$queryRaw`SELECT VERSION()`;
        return "mysql";
      } catch {
        try {
          await prisma.$queryRaw`SELECT @@VERSION`;
          return "sqlserver";
        } catch {
          return "postgresql";
        }
      }
    }
  }
}

export async function databaseProvider(prisma: PrismaLike): Promise<DatabaseProvider> {
  if (cachedProvider) {
    return cachedProvider;
  }

  cachedProvider = await detectDatabaseProvider(prisma);

  debug(`detected database provider: ${cachedProvider}`);

  return cachedProvider;
}

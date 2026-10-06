// Load .env here, before Jest starts its workers, so they inherit TEST_DB_URL. Variables already
// set in the environment win. CI has no .env and sets the variable directly.
try {
  process.loadEnvFile();
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}

/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: "node",
  transform: {
    "^.+\\.ts$": ["@swc/jest"],
  },
};

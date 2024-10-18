import { processDataDump } from "./challenge";

/**
 * This is the entry point for the challenge.
 * This will run your code.
 */
async function main() {
  try {
    await processDataDump();
    console.log("✅ Done!");
  } catch (error) {
    console.error("Error during data processing:", error);
  } finally {
    process.exit(0); // Exit explicitly to ensure process terminates
  }
}

main();
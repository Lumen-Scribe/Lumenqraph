import js from "@eslint/js";
import ts from "typescript-eslint";

export default ts.config(
  {
    ignores: ["dist", "node_modules"],
  },
  {
    files: ["**/*.ts"],
    extends: [js.configs.recommended, ...ts.configs.recommended],
    languageOptions: {
      parserOptions: {
        project: "./tsconfig.json",
      },
    },
    rules: {
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/no-explicit-any": "error",
    },
  },
);

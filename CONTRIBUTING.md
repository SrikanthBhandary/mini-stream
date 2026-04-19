# Contributing to MiniStream

First off, thank you for considering contributing to MiniStream! It’s people like you that make this project better. Whether it's a bug report, a performance improvement, or a new feature, your help is appreciated.

## Getting Started

1. **Fork the repo** and create your branch from `main`.
2. **Setup your environment:** Ensure you have Go 1.21+ installed.
3. **Run the tests** before making changes to ensure everything is stable:
   ```bash
   go test ./...
   ```

## Development Process

### 1. Opening an Issue
If you want to add a feature or fix a significant bug, please **open an issue first.** This helps us discuss the design before you spend time writing code that might not align with the project's architecture.

### 2. Pull Requests
* **One feature per PR:** Keep your changes focused.
* **Tests are mandatory:** If you add a feature, add a test. If you fix a bug, add a regression test. PRs without tests will be requested to include them.
* **Performance matters:** Since this is a storage engine, please run the benchmarks (`go test -bench=.`) if your change affects the ingestion or read path. If your PR slows down the engine, include a justification.

### 3. Coding Standards
* **Go Idioms:** Follow standard Go practices. Run `gofmt` before committing.
* **Documentation:** If you add a new public API or a complex internal function, please document it using standard Go doc comments (`// FunctionName ...`).
* **Commit Messages:** Use conventional commit messages:
    * `feat: add support for X`
    * `fix: resolve race condition in shard rotation`
    * `docs: update README with new benchmarks`
    * `perf: optimize disk I/O in ingestor`

## Code of Conduct

By participating in this project, you are expected to uphold our commitment to open, respectful communication. [Optional: Link to a Code of Conduct file if you have one].

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

### Need help?
If you are stuck or have questions about the internals of the storage engine, feel free to open a "Discussion" or ping me on the issue tracker. Happy coding!

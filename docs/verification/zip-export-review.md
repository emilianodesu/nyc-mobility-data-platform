# ZIP export review — 2026-11-09

This augmented export supersedes the earlier 25-document snapshot for transfer purposes. The earlier package-review report remains a historical record of that review, not the inventory of this larger bundle.

The current tree adds a portable entry point, agent guidance, researched structure rationale, arc42 coverage, deployment context, risks, glossary, and preserved research/design history. The preservation ledger enumerates original evidence and declared transformations. Current docs govern; historical reports/scripts do not provide execution instructions.

The root MANIFEST.json lists every exported file except itself, with exact bytes and SHA-256. It is independent of old source hashes recorded inside historical documents. The ZIP is validated by CRC, safe relative entry paths, extraction into a temporary directory, exact file-set equality and every exported hash. Active Markdown relative links are checked within the extracted root. Historical .md.txt links are preserved as source text rather than claimed to be navigable current links.

No pipeline is built, no dataset is downloaded, no dependencies are installed and no Git repository is initialized. Existing one-off research worksheets are preserved as .py.txt. All 29 runtime scenarios remain NOT RUN. Metadata, sampling limits, unresolved semantic questions and alternatives remain included. This handoff enables independent implementation planning; it does not guarantee an arbitrary agent will implement correctly or eliminate remaining empirical gates.

Research sources and adaptation choices are in [structure research](../delivery/structure-research.md). Review any future license/public distribution separately from this private archive.

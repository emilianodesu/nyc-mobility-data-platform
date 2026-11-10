# Research basis for repository/documentation structure

Researched 2026-11-09 using primary sources. No source mandates one universal data-engineering repository tree. This bundle deliberately adapts complementary conventions, with explicit coverage rather than asserting certification.

| Primary guidance | Observed convention | Application here |
| --- | --- | --- |
| [arc42 overview](https://arc42.org/overview/) | Twelve architecture topics covering goals through risks and glossary | [Coverage index](../architecture/arc42-index.md) maps every topic to owned content |
| [C4 diagrams](https://c4model.com/diagrams) | Use architecture views that add value; not every level is necessary | Context/process and dynamic/model diagrams, no fictional code diagram |
| [ADR guidance](https://adr.github.io/) | Record a significant decision and rationale/consequences | Five numbered records plus future supersession policy |
| [Cookiecutter Data Science](https://cookiecutter-data-science.drivendata.org/) | Separate root orientation, documentation, reference material, data and source | docs/ for design, references/ for preserved evidence; data/source folders deferred to implementation |

Cookiecutter is a data-science template, not proof that a monthly engineering pipeline requires notebooks, ML models or its default data layers. We adopt its useful separation and explicitly omit irrelevant empty scaffolding. arc42 supplies coverage, not a requirement to duplicate the same content in twelve files. Existing low-level documents remain authoritative and the coverage index links to them.

Initial tree: root README/START-HERE/AGENTS/CONTRIBUTING; docs/{architecture,reference,verification,evidence,delivery}; references/archive for historical reports, observations and archived inspection worksheets. Later implementation may add src/, sql/, tests/ and package/CI configuration following the implementation sequence. No template generator was run.

Evidence preservation: archived material is retained with content hashes and a transformation ledger. Personal absolute path prefixes are made portable, without altering technical observations. Inspection scripts are archived as .py.txt, not runnable project code. Historical file-internal hashes refer to historical artifacts; the export manifest is the authority for current archive bytes.

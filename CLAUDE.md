# Project Coding Guidelines for Claude Code

This document outlines the core development standards for this project. As a coding agent, you MUST adhere to these guidelines at all times. 

## 1. Code Style & Quality

* **PEP 8:** All Python code MUST be 100% PEP 8 compliant.
* **Clean Code:** Code must be readable, maintainable, and well-documented.
* **Single Responsibility Principle:** All functions, methods, and classes MUST be modular. A function should do one single task and do it well. Avoid monolithic functions or classes.


## 2. Development

* All file system operations (e.g., path handling, file I/O) **must** be cross-platform compatible with both Windows and Linux.
* Use libraries like `pathlib` for joining and manipulating paths, as they handle OS-specific separators (`\` vs. `/`) automatically.
* All new classes and functions must include docstrings using the **NumPy style**.


## 3. Data Validation & Type Checking

* **Pydantic:** All data models, API schemas (requests/responses), or configuration objects MUST use Pydantic for data validation and runtime type checking. 


## 4. Performance & Data Libraries

* **Prioritize Modern Libraries:** For all data manipulation and processing tasks, you MUST prioritize `polars` and `pyarrow` over `pandas`.
* **Vectorized Operations:** Always prefer vectorized operations over item-by-item (element-wise loop) operations when available. Vectorized operations leverage optimized, compiled code paths and significantly reduce overhead compared to Python-level loops.
* **Rationale:** The project goals are low latency and high memory efficiency. Always choose the most performant library for the task.


## 5. Logging

* **Use `logging` Module:** You MUST use Python's built-in `logging` module for all application-level logging (info, debug, warning, error, etc.).
* **No `print()`:** Do NOT use the `print()` function for logging. `print()` is only acceptable for direct console output in a CLI-based tool.
* **Use f-strings:** All string formatting, especially for log messages, MUST use f-strings (e.g., `logging.info(f'Processing item: {item_id}')`).


## 6. Testing

* **Use `pytest`:** All unit tests and integration tests MUST be written using the `pytest` framework.
* **Do Not Use `unittest`:** Do not use Python's built-in `unittest` module.
* **Fixtures:** Use `pytest` fixtures for test setup, teardown,  dependency injection, and creation & clean-up of temporary paths.
* **Coverage Goal:** All new code (patches, pull requests) MUST have 100% line and branch coverage. The overall project coverage should be maintained at or above 90%.
* **Test Quality:** Tests must be high-quality and include assertions for edge cases, error conditions, and "unhappy paths," not just the "happy path." Do not write tests simply to increase the coverage percentage.
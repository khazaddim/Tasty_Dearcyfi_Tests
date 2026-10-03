# Tasty_Dearcyfi_Tests
Testing DearCyGui / DearCyFi Integrations with TastyTrade In a collection of Demos

## DearCyFi demo setup

The demo currently depends on three coordinated branches/builds:

- [DearCyGui `Game_Controller_Build`](https://github.com/khazaddim/DearCyGui/tree/Game_Controller_Build) provides `dcg.PlotColorBars`, which the demo uses for colored volume bars. The DearCyFi dependency metadata does not currently declare this custom build.
- [DearCyFi `DCG-Mod`](https://github.com/khazaddim/DearCyFi/tree/DCG-Mod) provides the API expected by this demo, including the `gap_types` argument to `generate_fake_candlestick_data`.
- [DearCyFi_Demo `Test-DCG-Mod`](https://github.com/khazaddim/DearCyFi_Demo/tree/Test-DCG-Mod) calls that API. The demo is a submodule at `examples/DearCyFi_Demo`, configured to follow this branch.

For the tested Windows setup, use a CPython 3.14 free-threaded (`cp314t`) environment and the matching custom DearCyGui wheel. From the repository root in PowerShell:

```powershell
uv python install 3.14t
uv venv --python 3.14t --clear .venv
uv pip install --python .venv\Scripts\python.exe .\DCG_Custom_whl\dearcygui-0.1.8+multicontroller.1-cp314-cp314t-win_amd64.whl
uv pip install --python .venv\Scripts\python.exe "git+https://github.com/khazaddim/DearCyFi.git@DCG-Mod"
uv run --python .venv\Scripts\python.exe python .\examples\DearCyFi_Demo\DearCyFi_Demo.py
```

The custom wheel must be built for the interpreter and platform in use; the example wheel name above is specific to Windows x64 and CPython 3.14 free-threaded. Do not install the demo's `requirements.txt` as-is: it pins `dearcygui==0.1.7` and references DearCyFi without selecting `DCG-Mod`, so it can replace or conflict with the working custom setup.

### Cleanup follow-up

These branch-specific installs are temporary coordination between forks. Eventually, merge the needed DearCyFi changes from `DCG-Mod` into its `main` branch and the demo changes from `Test-DCG-Mod` into its `main` branch. Update DearCyFi's dependency metadata and CI to install/test the required `Game_Controller_Build` (or its published replacement), then update the demo requirements and this submodule to use the resulting main branches. That should make a clean environment reproducible without manual branch selection or an undocumented custom wheel.

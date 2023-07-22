# Sample CLI project in Python using PDM

Because debugging pyproject.toml is not fun and python imports are a mess.

This project was made to be as broad as possible in its use. If you don't need something, remove it.

## Capabilities
- Install
    - dependencies through pdm (`pdm install`)
        - into the local `__pypackages__` folder
            - to run your project as a python module
                - locally (`python -m my_project`)
                - in a docker container (see [Dockerfile](./Dockerfile))
            - to be accessed by your IDE for intellisense (see [.vscode/settings.json](./.vscode/settings.json))
            - to be accessed by your debug launch configuration (see [.vscode/launch.json](./.vscode/launch.json))
    - project through pip (`pip install .`)
        - to be used as a global CLI command (`run-my-project`)
- Build
    - through pdm (`pdm build`)
        - into the local `dist` folder
            - to be published on pypi
                - through twine (`twine upload dist/*`)

## Links
- [PDM Package Manager](https://pdm.fming.dev/)
- [Awesome Pyproject.toml](https://github.com/carlosperate/awesome-pyproject)

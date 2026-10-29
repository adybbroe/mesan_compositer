# Releasing mesan_compositer

prerequisites: `pip install build twine`


1. checkout main
2. pull from repo
3. run the tests and verify the package builds:

```
pytest
python -m build
python -m twine check dist/*
```
4. run `loghub` and update the `CHANGELOG.md` file:

```
loghub adybbroe/mesan_compositer --token <personal access token (see https://github.com/settings/tokens)>  -st v<previous version> -plg bug "Bugs fixed" -plg enhancement "Features added" -plg documentation "Documentation changes"
```

Don't forget to commit!

5. Create a tag with the new version number, starting with a 'v', eg:

```
git tag -a v0.1.1 -m "Version 0.1.1"
```

See [semver.org](http://semver.org/) on how to write a version number.


6. push changes to github `git push --follow-tags`

7. Verify the Github actions unit tests passed

8. Create a "Release" on GitHub by going to
   https://github.com/adybbroe/mesan_compositer/releases and clicking "Draft a new
   release". On the next page enter the newly created tag in the "Tag version"
   field, "Version X.Y.Z" in the "Release title" field, and paste the markdown
   from the changelog (the portion under the version section header) in the
   "Describe this release" box. Finally click "Publish release".

9. Verify the GitHub Actions release workflow succeeds and the release is on PyPI.

The release workflow uses PyPI Trusted Publishing, so no PyPI API token is stored in
GitHub. Before the first release, configure the PyPI project to trust the GitHub
Actions workflow `.github/workflows/deploy-sdist.yaml` and the `pypi` GitHub
environment.

10. Verify the published package in a clean environment, including the main console
entry point:

```
python -m pip install mesan_compositer==X.Y.Z
make_cloud_composite --help
```

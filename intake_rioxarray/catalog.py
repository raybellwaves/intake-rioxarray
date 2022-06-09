import fsspec
import xarray as xr
from intake.source.base import DataSource, Schema


class DataSourceMixin(DataSource):
    container = "test"
    partition_access = True

    def _get_schema(self):
        self.urlpath = self._get_cache(self.urlpath)[0]

        if self._ds is None:
            self._open_dataset()

            metadata = {
                "dims": dict(self._ds.dims),
                "data_vars": {
                    k: list(self._ds[k].coords) for k in self._ds.data_vars.keys()
                },
                "coords": tuple(self._ds.coords.keys()),
            }
            metadata.update(self._ds.attrs)
            self._schema = Schema(
                datashape=None,
                dtype=None,
                shape=None,
                npartitions=None,
                extra_metadata=metadata,
            )
        return self._schema

    def read(self):
        """Return a version of the xarray with all the data in memory"""
        self._load_metadata()
        return self._ds.load()


class RioXarraySource(DataSourceMixin):
    name = "rioxarray"

    def __init__(
        self,
        urlpath,
        chunks=None,
        combine=None,
        concat_dim=None,
        xarray_kwargs=None,
        metadata=None,
        path_as_pattern=True,
        storage_options=None,
        **kwargs
    ):
        self.path_as_pattern = path_as_pattern
        self.urlpath = urlpath
        self.chunks = chunks
        self.concat_dim = concat_dim
        self.combine = combine
        self.storage_options = storage_options or {}
        self.xarray_kwargs = xarray_kwargs or {}
        self._ds = None
        if isinstance(self.urlpath, list):
            self._can_be_local = fsspec.utils.can_be_local(self.urlpath[0])
        else:
            self._can_be_local = fsspec.utils.can_be_local(self.urlpath)
        super(RioXarraySource, self).__init__(metadata=metadata, **kwargs)

    def _open_dataset(self):
        url = self.urlpath

        kwargs = self.xarray_kwargs

        if self._can_be_local:
            url = fsspec.open_local(self.urlpath, **self.storage_options)
        else:
            # https://github.com/intake/filesystem_spec/issues/476#issuecomment-732372918
            url = fsspec.open(self.urlpath, **self.storage_options).open()

        self._ds = xr.open_dataset(url, engine="rioxarray", **kwargs)

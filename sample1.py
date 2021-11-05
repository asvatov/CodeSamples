import h5py
import datetime
import os
import glob
import pandas as pd
import collections
import calendar
import numpy as np

from barsim.data_provider.provider import DataProvider
from qtblib.time import cast_datetime_series, cast_datetime
from qtblib.utils.strtypes import cast_bytes, cast_unicode


class HDF5DataFrame(object):
    """Class to access QTB specific data archives stored in hdf5 files
    
       Each instance of HDF5Store represents a QTB data archive which is a single .h5 file containing following datasets:
       'data', 'timeline', 'dimname#1', 'dimname#2', ....
       0 dimension is reserved for timeline
    """
    DATA_DS = 'ds#data'
    DIM_DS = 'ds#dimname#'
    TIMELINE_DS = 'ds#timeline'
    HDF5_FILE_EXT = '.h5'

    def __init__(self, fn=''):
        """
        Args:
            fn (str): path to hdf5 file
        """
        self.fn = fn

        if not os.path.exists(os.path.dirname(fn)):
            os.makedirs(os.path.dirname(fn))

    def read_timeline(self):
        h5f = h5py.File(self.fn, 'r')
        data_dset = h5f[self.TIMELINE_DS]
        return data_dset[...]

    def read_dimension(self, dim_num):
        h5f = h5py.File(self.fn, 'r')
        data_dset = h5f['{}{}'.format(self.DIM_DS, dim_num)]
        return data_dset[...]

    def read(self, filters=None):
        """Read data

           filters is a list with length equal to number of dataset dimensions where each 
           element specify filtering criteria for corresponding dimension. 
           Filtering criteria could be 
            None: means no filtering on this dimension
            Slice:
            Dict: with 2 keys "start" and "end" and datetime.datetime values. Such a filter 
                could be applied to the first dimension (the timeline)
            list (or tuple): only columns with dimension labels included into the list will be read
        """

        h5f = h5py.File(self.fn, 'r')

        data_dset = h5f[self.DATA_DS]

        # read dimensions
        timeline, dims = None, [None] * (len(data_dset.shape) - 1)

        for i in range(len(data_dset.shape) - 1):
            try:
                dims[i] = h5f[self.DIM_DS + str(i + 1)][...]
                # convert dimension names to unicode
                dims[i] = np.array([cast_unicode(s) for s in dims[i]])
            except KeyError:
                pass

        # read timeline
        try:
            timeline = h5f[self.TIMELINE_DS][...]
        except KeyError:
            timeline = None

        if filters:
            # generate filtering indexes
            idx = []
            for i, f in enumerate(filters):
                if f is None:
                    idx.append(slice(None))
                elif isinstance(f, slice):
                    idx.append(f)
                elif isinstance(f, dict) and timeline is not None and \
                                set(f.keys()) == set(('start', 'end')) and \
                        isinstance(f['start'], datetime.datetime) and \
                        isinstance(f['end'], datetime.datetime):
                    idx.append(np.where(np.logical_and(timeline >= calendar.timegm(f['start'].timetuple()),
                                                       timeline <= calendar.timegm(f['end'].timetuple()))))
                elif isinstance(f, (list, tuple)):
                    idx.append([[i for i, x in enumerate(dims[i]) if x == e] for e in f])
                else:
                    raise ValueError('Unknown filtering values in position %i' % i)
            # filter the data
            data = data_dset[tuple(idx)]
        else:
            data = data_dset[...]

        return data, dims, timeline

    def _write_dict(self, dic, compression='gzip', compression_opts=9):
        f = h5py.File(self.fn, 'w')
        for key, field in dic.items():
            dset = f.create_dataset(key,
                                    field.shape,
                                    dtype=field.dtype,
                                    compression=compression,
                                    compression_opts=compression_opts,
                                    maxshape=list(np.tile(None, len(field.shape))))
            dset[...] = field
        f.close()

    def write(self, data, dims, timeline):
        # check for np.ndarray
        for arr in [data, timeline] + dims:
            if not isinstance(arr, np.ndarray):
                raise Exception('Data is not np.ndarray')

        # check for proper dimensions
        for arr in [timeline] + dims:
            if len(arr.shape) != 1:
                raise Exception('Timeline and dimension arrays should be 1-dimensional')
        if data.shape[0] != len(timeline):
            raise Exception('Data shape does not correspond to the timeline')
        for i in range(len(dims)):
            if data.shape[i + 1] != len(dims[i]):
                raise Exception('Data shape does not correspond to the dimension: {}'.format(str(i + 1)))
        
        dims = [np.array([cast_bytes(s) for s in dim]) for dim in dims]
        
        # write
        dic = {self.DATA_DS: data,
               self.TIMELINE_DS: timeline}
        for i, dim in enumerate(dims):
            dic['{}{}'.format(self.DIM_DS, i + 1)] = dim
        self._write_dict(dic)

    def append_dset(self, dset, dim_num, appended_data):
        """
         Append data to hdf5 dataframe along dim_num axis
        """
        # check the number of dimensions
        if len(dset.shape) != len(appended_data.shape):
            raise Exception('Appendable data has the wrong number of dimensions')

        # check that non-changed dimensions are the same
        for i in range(len(dset.shape)):
            if i != dim_num and appended_data.shape[i] != dset.shape[i]:
                raise Exception('Appendable data has the wrong shape of the old dimensions')

        # generate slices
        slices = [slice(None) for _ in range(len(dset.shape))]
        slices[dim_num] = slice(dset.shape[dim_num], dset.shape[dim_num] + appended_data.shape[dim_num])

        new_shape = np.array(dset.shape)
        new_shape[dim_num] += appended_data.shape[dim_num]
        dset.resize(new_shape)
        dset[tuple(slices)] = appended_data

    def append_dict(self, dic, dim_num):
         # check that all keys of store are present
        if dim_num == 0:
            meta_key = self.TIMELINE_DS
        else:
            meta_key = '{}{}'.format(self.DIM_DS, dim_num)

        keys2append = [meta_key, self.DATA_DS]
        if set(keys2append) != set(dic.keys()):
            raise Exception('Wrong data provided. Expected keys: {}'.format(str(keys2append)))
       
        # cast bytes for dims
        if 'dimname' in meta_key:
            dic[meta_key] = np.array([cast_bytes(x) for x in dic[meta_key]])
        f = h5py.File(self.fn, 'a')

        try:
            # check for np.ndarrays
            for key, data in dic.items():
                if not isinstance(data, np.ndarray):
                    raise Exception('Appendable data is not np.ndarray object: {}'.format(key))

            # check the number of dimensions
            if len(dic[meta_key].shape) != 1:
                raise Exception('Meta data should be 1-dimensional')

            if len(dic[self.DATA_DS].shape) != len(f[self.DATA_DS].shape):
                raise Exception('Data has wrong number of dimensions')

            # check that non-changed dimensions are the same
            dset = f[self.DATA_DS]
            for i in range(len(dset.shape)):
                if i != dim_num and dic[self.DATA_DS].shape[i] != dset.shape[i]:
                    raise Exception('Appendable data has the wrong shape of the old dimensions')

            # check that appendable data has the same appendable length
            append_length = dic[self.DATA_DS].shape[dim_num]
            if len(dic[meta_key]) != append_length:
                raise Exception('Different append lengths')

            self.append_dset(f[self.DATA_DS], dim_num, dic[self.DATA_DS])
            self.append_dset(f[meta_key], 0, dic[meta_key])
        except:
            f.close()
            raise

    def print_contents(self):
        f = h5py.File(self.fn, 'r')

        def print_item(name, item):
            print(name, item)

        f.visititems(print_item)
        f.close()


class HDF5DataBase(DataProvider):
    """ Database of 2 dimensional dataframes. There is only 0 and 1 dimension:
    0 is for the timeline and 1 for the items. All fields are stored in root directory.

    All data must have same timeline.
    All data must have same items if specified.
    """
    HDF5_FILE_EXT = '.h5'

    def __init__(self, root, allow_different_items=True):
        super().__init__()
        self.root = root
        if not os.path.exists(os.path.dirname(root)):
            os.makedirs(os.path.dirname(root))

        # dict(shape, timeline, fields, items)
        self._metadata = {}
        self.datetime_timeline = None

        self.allow_different_items = allow_different_items

    @property
    def metadata(self):
        if not self._metadata:
            self._load_metadata()
        return self._metadata

    def is_empty(self):
        return len(self.get_fields()) == 0

    # get metadata
    def get_fields(self):
        """
        Returns
        -------
        All available fields in the database
        """
        fns = glob.glob(os.path.join(self.root, '*' + self.HDF5_FILE_EXT))
        return [os.path.basename(fn)[:-len(self.HDF5_FILE_EXT)] for fn in fns]

    def get_items(self):
        """
        Returns
        -------
        Items (columns) of the database
        """
        if not self._metadata:
            self._load_metadata()
        return self._metadata['items']

    def get_timeline(self):
        """
        Returns
        -------
        Timeline of the database
        """
        if not self._metadata:
            self._load_metadata()
        return self._metadata['timeline']

    def add_fields(self, fields_dic):
        """ Add exchanges fields to the database.

        Parameters
        ----------
        fields_dic: dict
            {<field>: pd.DataFrame}
        """

        if not self.is_empty():
            self._load_metadata()

        for field, field_df in fields_dic.items():
            self._sync_metadata(field_df)

        for field, field_df in fields_dic.items():
            self.dump_field(field, field_df)

    def upd_fields(self, fields_dic):
        """ Update fields.

        Parameters
        ----------
        fields_dic: dict
            {<field>: pd.DataFrame}
        """
        self._reset_metadata()

        if self.is_empty():
            return self.add_fields(fields_dic)
        else:
            all_fields = self.get_fields()
            for field in fields_dic:
                new_field_df = fields_dic[field]
                if field not in all_fields:
                    self.dump_field(field, new_field_df)
                else:
                    field_df = self.load_field(field)
                    for col in new_field_df.columns:
                        field_df[col] = new_field_df[col]
                    self._sync_metadata(field_df)
                    self.dump_field(field, field_df)

    def append_fields(self, fields_dic):
        if self.is_empty():
            self._reset_metadata()
            return self.add_fields(fields_dic)
        else:
            last_dt = cast_datetime(self.metadata['timeline'][-1])
            self._reset_metadata()
            for field, field_df in fields_dic.items():
                data, dims, timeline = self._split_dataframe(field_df)

                first_dt = cast_datetime(timeline[0])

                if first_dt <= last_dt:
                    # todo: proper message
                    raise Exception('Bad input')

                hdf5dataframe = HDF5DataFrame(self._field_fn(field))
                hdf5dataframe.append_dict({'ds#timeline': timeline, 'ds#data': data}, 0)

    def remove_fields(self, fields):
        """ Remove specified fields. """
        for field in fields:
            field_fn = self._field_fn(field)
            if os.path.exists(field_fn):
                os.remove(field_fn)

    def field_apply(self, func):
        """ Update all current fields by applying func.

        Parameters
        ----------
        func
            Sample:
            def f(field, field_df):
                pass
        """
        self._reset_metadata()
        for field in self.get_fields():
            field_df = self.load_field(field)
            self._sync_metadata(field_df)
            self.dump_field(field, func(field, field_df))

    def dump_field(self, field, field_df):
        """ Dump field.
        Parameters
        ----------
        field: str
        field_df: pd.DataFrame

        Returns
        -------

        """
        data, dims, timeline = self._split_dataframe(field_df)
        hdf5dataframe = HDF5DataFrame(self._field_fn(field))
        hdf5dataframe.write(data, dims, timeline)

    def load_field(self, field, *args, **kwargs):
        """ Load field.

        Parameters
        ----------
        field: str
        args
        kwargs

        Returns
        -------

        """
        hdf5dataframe = HDF5DataFrame(self._field_fn(field))
        data, dims, timeline = hdf5dataframe.read()
        if self.datetime_timeline is None:
            self.datetime_timeline = cast_datetime_series(pd.Series(timeline))
        return pd.DataFrame(data=data.reshape(len(timeline), -1), index=self.datetime_timeline, columns=dims[0])

    def _field_fn(self, field):
        return os.path.join(self.root, field + self.HDF5_FILE_EXT)

    def _load_metadata(self):
        """ Read the smallest field to fetch metadata. """
        if self._metadata or self.is_empty():
            return
        field_fns = [self._field_fn(field) for field in self.get_fields()]
        smallest_fn = sorted(field_fns, key=os.path.getsize)[0]
        field = os.path.basename(smallest_fn.replace('.h5', ''))
        hdf5dataframe = HDF5DataFrame(self._field_fn(field))

        self._metadata['items'] = [cast_unicode(x) for x in hdf5dataframe.read_dimension(1).tolist()]
        self._metadata['timeline'] = hdf5dataframe.read_timeline().tolist()
        field_df = self.load_field(field)
        self._sync_metadata(field_df)

    def _cast_timestamp(self, x):
        if hasattr(x, 'timestamp') and isinstance(x.timestamp, collections.Callable):
            return x.timestamp()
        elif type(x) == int:
            return x
        else:
            raise ValueError('Non-timesetamp type')

    def _split_dataframe(self, df):
        """ Split DataFrame to a 3-item tuple: [data, dims, timeline].
        Parameters
        ----------
        df: pd.DataFrame

        Returns
        -------
        [data, dims, timeline]
        """
        return df.values, [np.array(df.columns)], np.array([self._cast_timestamp(x) for x in df.index])

    def _sync_metadata(self, field_df):
        """ Synchronize metadata with current field_df. If metadata of the exchanges dataframe is wrong - an Exception will be raise.

        Parameters
        ----------
        field_df: pd.DataFrame
        """
        data, dims, timeline = self._split_dataframe(field_df)
        if not self._metadata:
            self._metadata = {'timeline': list(timeline), 'items': list(dims[0])}
        else:
            if not np.array_equal(timeline, np.array(self.metadata['timeline'])):
                raise Exception('Different timelines are not supported')

            if not self.allow_different_items and list(dims[0]) != self.metadata['items']:
                raise Exception('Different items are not allowed')

    def _reset_metadata(self):
        self._metadata = {}
        self.datetime_timeline = None


if __name__ == '__main__':
    df = HDF5DataFrame(r"C:\Users\xiaomi\YandexDisk\IT\Python\pycharm\barsim\data\20180319_1d\close.h5")
    print(df.read_timeline())
    print(df.read_dimension(1))

    db = HDF5DataBase(r"C:\Users\xiaomi\YandexDisk\IT\Python\pycharm\barsim\data\20180319_1d")
    print(db.load_field('close'))

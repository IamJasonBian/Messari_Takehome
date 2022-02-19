### Shift Function Notes ###

def shift(self, periods=1, freq=None, axis=0, fill_value=None):
    * periods:
    * frequency:
    * axis:
    * fill_value:
    
     if freq is not None or axis != 0:
          return self.apply(lambda x: x.shift(periods, freq, axis, fill_value))
          
   Function calls itself if the axis is set to 0, we will run shift again but with 
   
     ids, _, ngroups = self.grouper.group_info
     res_indexer = np.zeros(len(ids), dtype=np.int64)
     
   Function will return ids and number of groups from the grouper. Grouper create the framework to extract relevent groups within pandas
   
     libgroupby.group_shift_indexer(res_indexer, ids, ngroups, periods)
     obj = self._obj_with_exclusions
     
   Function calculates the res_indexer via c Cpython code
   
      res = obj._reindex_with_indexers(
            {self.axis: (obj.axes[self.axis], res_indexer)},
            fill_value=fill_value,
            allow_dups=True,

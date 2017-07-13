#
# Post book building trade extractor.
#
# Usage: python extract_trades.py YYYYMMDD
#
# This extractor assumes all needed data files are local.  The network should never be used to
# retrieve missing data, which means this code will fail on missing data.
#

# Standard imports
import pandas as pd
from pandas import DataFrame
import sys
import os
import gzip
import datetime
import io

# EveKit imports
from evekit.marketdata import OrderBook
from evekit.marketdata import MarketHistory


def get_market_history(history_date, types, regions):
    """
    Retrieve market history for the given types and regions five days prior to the target date.

    :param history_date: date before which market history will be extracted
    :param types: list of type IDs for which market history will be extracted
    :param regions: list of region IDs for which market history will be extracted
    :returns: a MarketHistory object converted to a DataFrame with the requested information.
    """
    date_range = pd.date_range(history_date - datetime.timedelta(days=6), history_date - datetime.timedelta(days=1))
    return MarketHistory.get_data_frame(dates=date_range, types=types, regions=regions,
                                        config=dict(local_storage=".", tree=False, skip_missing=False,
                                                    verbose=False, use_online=False))


def read_bulk_file(file_date, batch_callback, end_callback, parent_dir=".", is_tree=False):
    """
    Iterate through all snapshots for all types contained in an interval file
    for the target date.  Each type is passed to a provided callback function
    for processing.

    :param file_date: date of interval file to process.
    :param batch_callback: callback to invoke for each order book.  The callback should have
    signature cb(type_id, DataFrame) where the DataFrame is a Panda's DataFrame containing the order book
    for this type.
    :param end_callback: callback to invoke when we've finished processing the bulk file.
    :param parent_dir: location of market data files.
    :param is_tree: if tree, then assume market data files are stored at the path <parent_dir>/YYYY/MM/DD
    :returns: True if all processing completes successfully, False otherwise.
    """
    path_string = "%04d/%02d/%02d" % (file_date.year, file_date.month, file_date.day)
    date_string = "%04d%02d%02d" % (file_date.year, file_date.month, file_date.day)
    bulk_file = parent_dir + "/" + (path_string if is_tree else "") + "/interval_" + date_string + "_5.bulk"
    index_file = parent_dir + "/" + (path_string if is_tree else "") + "/interval_" + date_string + "_5.index.gz"
    if (not os.path.exists(bulk_file)) or (not os.path.exists(index_file)):
        return False
    try:
        max_offset = os.stat(bulk_file).st_size
        index_map = OrderBook.__read_index__(open(index_file, 'rb'), max_offset)
        # Scan the entire file, skipping types we don't care about
        sorted_map = []
        scanned = 0
        for x in index_map.keys():
            sorted_map.append(dict(type=x, start=index_map[x][0], end=index_map[x][1]))
        sorted_map = sorted(sorted_map, key=lambda k: k['start'])
        fd = open(bulk_file, 'rb')
        for next_type in sorted_map:
            type_id = next_type['type']
            start = next_type['start']
            end = next_type['end']
            buff = fd.read(end - start + 1)
            scanned += 1
            ps = gzip.GzipFile(fileobj=io.BytesIO(buff))
            next_book = OrderBook(file_date, ps=ps)
            next_book.fill_gaps()
            order_list = []
            for region_id in next_book.region.keys():
                for next_snap in next_book.region[region_id]:
                    snap_time = next_snap.snapshot_time
                    for next_order in next_snap.bid + next_snap.ask:
                        to_dict = next_order.__dict__
                        to_dict['date'] = snap_time
                        to_dict['type_id'] = type_id
                        to_dict['region_id'] = region_id
                        order_list.append(to_dict)
            df = DataFrame(order_list, [x['date'] for x in order_list])
            batch_callback(type_id, df)
            ps.close()
        fd.close()
    except OSError:
        return False
    end_callback()
    return True


def infer_trades(type_set, region_set, order_book_full, market_history_full):
    """
    Infer set of trades for the given types and regions from the given order book using volume
    thresholds computed from the given market history dataframe.

    :param type_set: the set of types for which trades will be inferred.
    :param region_set: the set of regions for which trades will be inferred.
    :param order_book_full: the order book dataframe from which trades will be inferred.
    :param market_history_full: the market history dataframe from which trades will be inferred.
    :returns: the list of inferred trades.
    """
    inferred_trades = []
    count = 0
    for next_region in region_set:
        if count % 10 == 0:
            print(".", end="", flush=True)
        order_book = order_book_full[order_book_full.region_id == next_region]
        market_history = market_history_full[market_history_full.region_id == next_region]
        count += 1
        
        # Compute per-type volume threshold
        volume_threshold_map = {}
        threshold_ratio = 0.04
        for next_type in type_set:
            by_type = market_history[market_history.type_id == next_type]
            thresh_series = by_type.volume.rolling(window=5, center=False).mean()
            if len(thresh_series) == 0:
                volume_threshold_map[next_type] = 0
            else:
                volume_threshold_map[next_type] = thresh_series[-1] * threshold_ratio
    
        # We need to iterate over consecutive order book snapshots looking for changes between snapshots.
        # To do this, we'll group by snapshot time as we did in Example 2, then iterate over 
        # consecutive pairs of groups.
        snap_list = list(order_book.groupby(order_book.index))
        snap_pairs = zip(snap_list, snap_list[1:])
        for current, next in snap_pairs:
            current_snap = current[1]
            next_snap = next[1]
            # First, look for orders present in both snapshots but have their volume changed
            merged = pd.merge(current_snap, next_snap, on="order_id")
            changed_orders = merged[merged.volume_x != merged.volume_y]
            for next_change in changed_orders.index:
                # Create the trade object
                next_line = changed_orders.loc[next_change]
                if next_line.type_id_x not in type_set:
                    continue
                amount = next_line.volume_x - next_line.volume_y 
                location = next_line.location_id_x
                if next_line.buy_x and next_line.order_range_x != 'station':
                    # For buy orders, we can't be certain where the trade occurred unless the
                    # buy is limited to a station
                    location = None
                inferred_trades.append({
                    'timestamp': next[0],
                    'region_id': next_region,
                    'type_id': next_line.type_id_x,
                    'actual': True,
                    'buy': next_line.buy_x, 
                    'order_id': next_line.order_id, 
                    'price': next_line.price_y,
                    'volume': amount, 
                    'location': str(location)})                
            # Second, look for orders which are removed between snapshots.
            removed_orders = set(current_snap.order_id).difference(set(next_snap.order_id))
            for order_id in removed_orders:
                next_line = current_snap[current_snap.order_id == order_id].loc[current[0]]
                if next_line.type_id not in type_set:
                    continue
                # If the volume of a removed order does not exceed the threshold, then keep it as a trade.
                volume_threshold = volume_threshold_map[next_line.type_id]
                if next_line.volume <= volume_threshold:
                    location = next_line.location_id
                    if next_line.buy and next_line.order_range != 'station':
                        # See above
                        location = None
                    inferred_trades.append({
                        'timestamp': next[0],
                        'region_id': next_region,
                        'type_id': next_line.type_id,
                        'actual': False,
                        'buy': next_line.buy, 
                        'order_id': order_id, 
                        'price': next_line.price,
                        'volume': next_line.volume, 
                        'location': str(location)})                
    # Return result
    return inferred_trades


def infer_trades_helper(next_types, compute_date):
    """
    A simple helper function which prepares a list of order books for trade inferral.
    The list of order books is collected from the bulk interval reader, then
    aggregated into a single order book dataframe from which trades will be inferred.
    We also determine the appropriate market history needed to filter large trades
    by volume.

    :param next_types: A list of pairs (type_id, order_book_df) from which trades will be inferred.
    :param compute_date: date for trade extraction
    :returns: the list of inferred trades.
    """
    full_type_list = [x[0] for x in next_types]
    full_book_list = [x[1] for x in next_types]
    full_region_list = set()
    for x in full_book_list:
        for y in x['region_id'].unique():
            full_region_list.add(y)
    full_region_list = list(full_region_list)
    market_history = get_market_history(compute_date, full_type_list, full_region_list)
    if len(full_book_list) > 1:
        full_book = full_book_list[0].append(full_book_list[1:])
    else:
        full_book = full_book_list[0]
    return infer_trades(full_type_list, full_region_list, full_book, market_history)
    

def write_trades(fobj, trade_list):
    """
    Output the list of trades to the given file object.

    :param fobj: file object where output will be sent.
    :param trade_list: list of inferred trades to output.
    """
    type_list = set()
    for x in trade_list:
        type_list.add(x['type_id'])
    type_list = list(type_list)
    type_list.sort()
    for next_type in type_list:
        trades = [x for x in trade_list if x['type_id'] == next_type]
        trades.sort(key=lambda y: y['timestamp'])
        region_list = set()
        for x in trades:
            region_list.add(x['region_id'])
        region_list = list(region_list)
        region_list.sort()
        fobj.write(str(next_type) + "\n")
        fobj.write(str(len(region_list)) + "\n")
        for next_region in region_list:
            region_trades = [x for x in trades if x['region_id'] == next_region]
            fobj.write(str(next_region) + "\n")
            fobj.write(str(len(region_trades)) + "\n")
            for tt in region_trades:
                fobj.write("{0:d},{1:s},{2:s},{3:d},{4:.2f},{5:d},{6:s}\n".format(
                    int(tt['timestamp'].timestamp() * 1000), str(tt['actual']), str(tt['buy']),
                    tt['order_id'], tt['price'], tt['volume'], tt['location']))


def extract_trades(compute_date, fout):
    # Setup
    type_count = 0
    next_types = []

    # Process a batch of trades
    def process_batch(type_list):
        print("Processing type batch", end="", flush=True)
        inferred = infer_trades_helper(type_list, compute_date)
        write_trades(fout, inferred)
        print("done", flush=True)

    # Callback for collecting trade batches
    def find_trades(type_id, df):
        nonlocal type_count, next_types
        global type_batch_size
        if type_count < type_batch_size:
            if type_count == 0:
                print("Collecting types", end="", flush=True)
            if type_count % 20 == 0:
                print("+", end="", flush=True)
            if len(df) > 0:
                next_types.append((type_id, df))
                type_count += 1
        else:
            print("done")
            process_batch(next_types)
            next_types = []
            type_count = 0

    # Callback for handling end of file.  This function makes
    # sure we write out any partial batch which might remain.
    def end_read():
        global type_batch_size
        if type_count < type_batch_size:
            print("done")
            print("Processing type batch", end="", flush=True)
            last_batch = infer_trades_helper(next_types, compute_date)
            write_trades(fout, last_batch)
            print("done", flush=True)

    if not read_bulk_file(compute_date, find_trades, end_read):
        print("Failed to read bulk file, output is likely invalid")
        sys.exit(1)


if __name__ == "__main__":
    # Date for which trades will be extracted
    target_date = datetime.datetime.strptime(sys.argv[1], "%Y%m%d")
    type_batch_size = 200
    if len(sys.argv > 2):
        type_batch_size = int(sys.argv[2])
    
    # Extract all trades
    filename = "trades_allregions_{0:04d}{1:02d}{2:02d}".format(target_date.year, target_date.month, target_date.day)
    with open(filename, 'w') as outfile:
        extract_trades(target_date, outfile)

    sys.exit(0)

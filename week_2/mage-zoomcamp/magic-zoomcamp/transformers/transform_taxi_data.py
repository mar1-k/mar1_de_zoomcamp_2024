if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    return_df = data

    #print("Rows with zero passengers:", data['passenger_count'].isin([0]).sum())
    print(f"Preprocessing rows with zero passengers: { data[['passenger_count']].isin([0]).sum() }")
    print(f"Preprocessing rows with zero trip distance: { data[['trip_distance']].isin([0]).sum() }")

    #Add a new column for lpep_pickup_date - Doing this in the export now
    #return_df['lpep_pickup_date'] =  data['lpep_pickup_datetime'].dt.strftime('%Y-%m-%d')

    #Remove ride data for passenger_count = 0
    return_df = return_df[return_df['passenger_count'] > 0]

    #Remove ride data for trip_distance = 0
    return_df = return_df[return_df['trip_distance'] > 0]

    return return_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, ' There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, ' There are rides with zero trip distance'

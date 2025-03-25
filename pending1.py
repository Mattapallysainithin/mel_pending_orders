import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Write directly to the app
st.title("🥤 Pending Smoothie Orders 🥤")
st.write("Orders that need to be filled.")

# Connect to Snowflake
session = get_active_session()

# Fetch unfilled orders
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == False).select(
    col("INGREDIENTS"),
    col("NAME_ON_ORDER"),
    col("ORDER_FILLED")
).to_pandas()

# Editable dataframe
editable_df = st.data_editor(my_dataframe, key='editable_orders')

# Submit button to save changes
if st.button("Submit Changes"):
    # Identify changes
    if not editable_df.equals(my_dataframe):
        for index, row in editable_df.iterrows():
            # Identify changed rows by comparing values
            if not all(row == my_dataframe.loc[index]):
                updated_ingredients = row['INGREDIENTS']
                updated_name = row['NAME_ON_ORDER']
                updated_filled = row['ORDER_FILLED']

                # Update Snowflake with modified entries
                session.sql(f"""
                    UPDATE smoothies.public.orders
                    SET INGREDIENTS = '{updated_ingredients}',
                        NAME_ON_ORDER = '{updated_name}',
                        ORDER_FILLED = {str(updated_filled).upper()}
                    WHERE INGREDIENTS = '{my_dataframe.at[index, 'INGREDIENTS']}'
                    AND NAME_ON_ORDER = '{my_dataframe.at[index, 'NAME_ON_ORDER']}'
                """).collect()

        st.success("Order details updated successfully! ✅")
        st.rerun()  # Refresh data after updates
    else:
        st.info("No changes detected. 😊")

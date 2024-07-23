# Re-importing necessary libraries
import pandas as pd
import numpy as np

# Correcting and completing the mock data creation with all necessary details
mock_data_corrected = {
    "name": [
        "Hellmann's Light Mayonnaise Squeezy 650Ml", "Express Tesco Egg Custard Tart 2 Pack",
        "Tesco Loose Red Peppers(C)", "Yorkie Raisin & Biscuit Chocolate Bar 44g",
        "San Pellegrino Sparkling Natural Mineral Water...", "Tesco Plant Chef Roasted Vegetable & Pesto Wrap",
        "Coca-Cola Classic 2L", "Walkers Classic Variety Multipack Crisps 22 X 25G",
        "Tesco British Whole Milk 3.408L", "Cadbury Dairy Milk Chocolate Bar 360G",
        "Heinz Baked Beans 415g", "Warburtons Toastie White Bread", 
        "Lurpak Slightly Salted Butter 250g", "Kellogg's Corn Flakes 720g",
        "McVitie's Digestive Biscuits 400g", "PG Tips Original 240 Tea Bags",
        "Birds Eye 2 Chicken Shop Sizzler Fillet Burgers", "Fairy Original Washing Up Liquid 780ml",
        "Andrex Classic Clean Toilet Tissue 9 Rolls", "Ariel Allin1 Pods Washing Capsules 36 Washes"
    ],
    "quantity": [1, 1, 1, 1, 1, 1, 2, 1, 3, 1, 2, 1, 1, 1, 2, 1, 1, 2, 1, 1],
    "channel": ["IN_STORE"]*20,
    "price": [3.1, 0.95, 0.65, 0.55, 0.92, 1.93, 2.50, 3.00, 1.45, 2.99, 0.85, 1.20, 2.25, 2.75, 1.50, 6.00, 3.50, 1.75, 4.50, 9.99],
    "timeStamp": [
        "2024-01-26 17:30:00", "2024-01-26 17:32:00",
        "2024-01-22 09:15:00", "2024-01-18 18:45:00",
        "2024-01-18 18:47:00", "2024-01-17 12:30:00",
        "2024-01-26 17:31:00", "2024-01-22 09:16:00",
        "2024-01-19 11:00:00", "2024-01-17 12:31:00",
        "2024-01-25 17:30:00", "2024-01-23 09:15:00",
        "2024-01-21 18:45:00", "2024-01-20 12:30:00",
        "2024-01-25 17:32:00", "2024-01-23 09:17:00",
        "2024-01-21 18:46:00", "2024-01-20 12:32:00",
        "2024-01-24 12:30:00", "2024-01-22 18:45:00"
    ],
    "customer_email": [
        "benedict.s@hotmail.co.uk", "benedict.s@hotmail.co.uk",
        "alex.j@example.com", "alex.j@example.com",
        "chris.t@example.net", "chris.t@example.net",
        "jane.d@example.com", "jane.d@example.com",
        "dave.h@example.org", "dave.h@example.org",
        "emily.f@example.com", "emily.f@example.com",
        "frank.g@example.com", "frank.g@example.com",
        "grace.h@example.com", "grace.h@example.com",
        "harry.i@example.com", "harry.i@example.com",
        "irene.j@example.com", "irene.j@example.com"
    ],
    "basketValueGross": [4.05, 4.05, 17.34, 3.4, 3.4, 4.65, 5.00, 3.00, 4.35, 2.99, 5.10, 6.40, 4.75, 8.25, 3.00, 6.00, 7.00, 6.25, 8.50, 19.98],
    "purchaseType": ["IN_STORE"]*20,
    "overallBasketSavings": [0.9, 0.9, 0.3, 1.6, 1.6, np.nan, 0.5, 0, 0.55, 0.3, 0.4, 0.2, 0.5, 0.75, np.nan, 0.5, 0.3, 0.4, 0.6, 1.0],
    "storeId": [5599, 5599, 5599, 6485, 6485, 5599, 5599, 5599, 6485, 6485, 5599, 5599, 6485, 6485, 5599, 5599, 6485, 6485, 5599, 6485],
    "storeAddress": [
        "226-228 Commercial Rd, Whitechapel", "226-228 Commercial Rd, Whitechapel",
        "226-228 Commercial Rd, Whitechapel", "125 Tooley Street",
        "125 Tooley Street", "226-228 Commercial Rd, Whitechapel",
        "226-228 Commercial Rd, Whitechapel", "226-228 Commercial Rd, Whitechapel",
        "125 Tooley Street", "125 Tooley Street",
        "226-228 Commercial Rd, Whitechapel", "226-228 Commercial Rd, Whitechapel",
        "125 Tooley Street", "125 Tooley Street",
        "226-228 Commercial Rd, Whitechapel", "226-228 Commercial Rd, Whitechapel",
        "125 Tooley Street", "125 Tooley Street",
        "226-228 Commercial Rd, Whitechapel", "125 Tooley Street"
    ],
    "storeName": [
        "Commercial Rd Express", "Commercial Rd Express",
        "Commercial Rd Express", "Tooley Street Express",
        "Tooley Street Express", "Commercial Rd Express",
        "Commercial Rd Express", "Commercial Rd Express",
        "Tooley Street Express", "Tooley Street Express",
        "Commercial Rd Express", "Commercial Rd Express",
        "Tooley Street Express", "Tooley Street Express",
        "Commercial Rd Express", "Commercial Rd Express",
        "Tooley Street Express", "Tooley Street Express",
        "Commercial Rd Express", "Tooley Street Express"
    ],
    "storeFormat": ["Express"]*20,
    "type": ["Amex"]*20,
    "amount": [4.05, 4.05, 17.34, 3.4, 3.4, 4.65, 5.00, 3.00, 4.35, 2.99, 5.10, 6.40, 4.75, 8.25, 3.00, 6.00, 7.00, 6.25, 8.50, 19.98]
}

# Convert the corrected dictionary to a DataFrame
mock_df_corrected = pd.DataFrame(mock_data_corrected)

# Convert timestamps to datetime objects
mock_df_corrected['timeStamp'] = pd.to_datetime(mock_df_corrected['timeStamp'])

# Extract time-based features
mock_df_corrected['minute'] = mock_df_corrected['timeStamp'].dt.minute
mock_df_corrected['hour'] = mock_df_corrected['timeStamp'].dt.hour
mock_df_corrected['weekday'] = mock_df_corrected['timeStamp'].dt.weekday  # Monday=0, Sunday=6
mock_df_corrected['day_of_month'] = mock_df_corrected['timeStamp'].dt.day

# Now, you can drop the 'timeStamp' column to avoid dtype issues in TPOT
mock_df_corrected.drop(['timeStamp'], axis=1, inplace=True)
friendship_info = {
    "benedict.s@hotmail.co.uk": ["jane.d@example.com", "emily.f@example.com"],  # Similar shopping times
    "alex.j@example.com": ["frank.g@example.com", "grace.h@example.com"],  # Similar shopping times
    "chris.t@example.net": ["harry.i@example.com"],  # Similar shopping times
    "jane.d@example.com": ["benedict.s@hotmail.co.uk", "emily.f@example.com"],  # Similar shopping times
    "dave.h@example.org": ["irene.j@example.com"],  # Similar shopping times
    "emily.f@example.com": ["benedict.s@hotmail.co.uk", "jane.d@example.com"],  # Similar shopping times
    "frank.g@example.com": ["alex.j@example.com"],  # Similar shopping times
    "grace.h@example.com": ["alex.j@example.com"],  # Similar shopping times
    "harry.i@example.com": ["chris.t@example.net"],  # Similar shopping times
    "irene.j@example.com": []}

# Extract unique email addresses (individuals) from the mock dataset
individuals = mock_df_corrected['customer_email'].unique()

# Initialize a DataFrame with zeros, using individuals as both rows and columns
friendship_matrix = pd.DataFrame(0, index=individuals, columns=individuals)

# Populate the matrix based on the friendship_info dictionary
for person, friends in friendship_info.items():
    for friend in friends:
        friendship_matrix.loc[person, friend] = 1
        friendship_matrix.loc[friend, person] = 1  # Friendship is mutual

# Assuming mock_df_corrected is our transaction dataset
features = mock_df_corrected.drop(['customer_email'], axis=1)  # Excluding email for feature encoding
features_encoded = pd.get_dummies(features)
# Assuming `features_encoded` is your encoded transaction data
# Let's first ensure the customer email is included for aggregation
mock_df_corrected['customer_email'] = mock_df_corrected['customer_email'].astype('category')
features_with_email = pd.concat([mock_df_corrected['customer_email'], features_encoded], axis=1)

# Aggregate features by customer email
individual_features = features_with_email.groupby('customer_email').mean()

# Ensure the index is consistent for later operations
individual_features = individual_features.reindex(friendship_matrix.index)
X_train_list = []
y_train_list = []

for i, email_i in enumerate(friendship_matrix.index):
    for j, email_j in enumerate(friendship_matrix.columns):
        if i < j:  # Avoid duplicate pairs and self-pairing
            # Combine features of both individuals in the pair
            features_pair = pd.concat([individual_features.loc[email_i], individual_features.loc[email_j]], axis=0).to_list()
            X_train_list.append(features_pair)
            # Corresponding friendship status
            y_train_list.append(friendship_matrix.loc[email_i, email_j])

# Convert lists to DataFrame and Series for X_train and y_train
X_train = pd.DataFrame(X_train_list)
y_train = pd.Series(y_train_list)

friendship_matrix
from tpot import TPOTClassifier
import sklearn.model_selection

# Assuming your X_train and y_train are correctly prepared
X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X_train, y_train, 
                                                                            test_size=0.25, random_state=42)

# Initialize TPOT classifier
tpot = TPOTClassifier(generations=5, population_size=50, verbosity=2, random_state=42)

# Fit the TPOT model to your data
tpot.fit(X_train, y_train)

# Evaluate the model on the test set
print(tpot.score(X_test, y_test))

# Optionally, export the pipeline to a Python file
# tpot.export('tpot_best_pipeline.py')

print(tpot.score(X_test, y_test))



###Bits to add
    # How frequently a customer buys - will have to calculate from basket data then put into customer row
    # Weight it all to include last X sets of transactions 
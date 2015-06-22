(: Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
   
     http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License. :)

(: EXRT Query see README.md for full details. :)

declare default element namespace "http://tpox-benchmark.com/custacc";
for $cust in db2-fn:xmlcolumn('CUSTACC.CADOC')/Customer[@id >= |1 and @id < |2  + |3]  
return
    element Profile{
        attribute CustomerId {$cust/@id},
        $cust/Mnemonic,
        element ShortNames{
            $cust/ShortNames/ShortName
        },
        element Name {
            $cust/Name/Title,
            $cust/Name/FirstName,
            $cust/Name/MiddleName,
            $cust/Name/LastName,
            $cust/Name/Suffix 
        },
        $cust/DateOfBirth,
        $cust/Gender,
        $cust/Nationality,
        $cust/CountryOfResidence,
        element Languages{ $cust/Languages/Language },
        element Addresses{
            for $ad in $cust/Addresses/Address
            return 
                element Address{
                    attribute primary {$ad/@primary},
                    attribute type {$ad/@type},
                    element gStreet { $ad/gStreet/Street },
                    $ad/POBox,
                    $ad/City,
                    $ad/PostalCode,
                    $ad/State,
                    $ad/Country,
                    $ad/CityCountry,
                    element Phones{ $ad/Phones/Phone }      
            },
            element EmailAddresses { $cust/Addresses/EmailAddresses/Email }
        },
        element BankingInfo {
            $cust/BankingInfo/CustomerSince,
            $cust/BankingInfo/PremiumCustomer,
            $cust/BankingInfo/CustomerStatus,
            $cust/BankingInfo/LastContactDate,
            $cust/BankingInfo/ReviewFrequency,
            element Online {
                $cust/BankingInfo/Online/Login,
                element Pin{
                    element EncryptedData {
                        attribute Type { $cust/BankingInfo/Online/Pin/EncryptedData/@Type },
                        element CipherData { $cust/BankingInfo/Online/Pin/EncryptedData/CipherData/CipherValue }
                    }
                },
                element Trading-password {
                    element EncryptedData {
                        attribute Type { $cust/BankingInfo/Online/Trading-password/EncryptedData/@Type },
                        element CipherData { $cust/BankingInfo/Online/Trading-password/EncryptedData/CipherData/CipherValue }
                    }
                }
            },
            element Tax {
                $cust/BankingInfo/Tax/TaxID,
                element SSN {
                    element EncryptedData {
                        attribute Type { $cust/BankingInfo/Tax/SSN/EncryptedData/@Type },
                        element CipherData { $cust/BankingInfo/Tax/SSN/EncryptedData/CipherData/CipherValue }
                    }
                },
                $cust/BankingInfo/Tax/TaxRate
            },
            $cust/BankingInfo/Currency
        },
        element Accounts {
            for $a in $cust/Accounts/Account
            return
                element Account{
                    $a/Category,
                    $a/AccountTitle,
                    $a/ShortTitle,
                    $a/Mnemonic,
                    $a/Currency,
                    $a/CurrencyMarket,
                    $a/OpeningDate,
                    $a/AccountOfficer,
                    $a/LastUpdate,
                    element Balance {
                        $a/Balance/OnlineActualBal,
                        $a/Balance/OnlineClearedBal,
                        $a/Balance/WorkingBalance
                    },
                    $a/Passbook,
                    element gValueDate { $a/gValueDate/mValueDate },
                    $a/ChargeCcy,
                    $a/InterestCcy,
                    $a/AllowNetting,
                    element gInputter { $a/gInputter/Inputter },
                    $a/Holdings
                }
        }
    }

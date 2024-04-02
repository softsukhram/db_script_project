/*
 * $Id: AccountingDAO.cs,v 1.25 2008/02/14 20:24:59 tgrotenh Exp $
 * Software is property of Shape.Net, Inc. 2003,2005
 */

 
using System;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Collections;
using log4net;
using Telerik.Web.UI;
using ShapeDotNet.Crosslayer;
using ShapeDotNet.Crosslayer.Enums;
using ShapeDotNet.Util;
using ShapeDotNet.Util.Crypto;
using ShapeDotNet.Logging;
using ShapeDotNet.Util.Configuration;

namespace ShapeDotNet.Access
{
	/// <summary>
	/// This class encapsulates the procedure used to write and retrieve
	/// accountingVO information. This 
	/// class retrieves the connection string from the web.config
	/// file within each method.  It does not maintain state.
	/// </summary>
	public class AccountingDAO
	{
		private static readonly ILog log = LogManager.GetLogger( typeof( AccountingDAO )  );
		
		public AccountingDAO()
		{
		}

        public ScheduledPaymentVO retrieveUpcomingSchedulePayment(int accessCode)
        {
            if (accessCode <= 0)
            {
                // Should throw an exception and log this.
                string msg = string.Format("accessCode [{0}] is not valid.", accessCode);
                log.Error(msg);
                throw new Exception(msg);
            }

            SqlConnection connection = null;
            SqlCommand command = null;
            SqlDataReader rs = null;
            SqlParameter idParam = null;
            ScheduledPaymentVO vo = null;

            // 1. Get the connection string.
            PaymentBillingTypeEnumDatabaseMapper paymentBillingTypeDbMapper = PaymentBillingTypeEnumDatabaseMapper.getInstance();
            ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();

            // 1. Get the connection string.
            string connectionParam = ShapeConnection.retrieveConnection(ConfigurationManager.AppSettings["SQLDBConnString"]);
            MembershipStatusEnumDatabaseMapper membershipStatusDbMapper = MembershipStatusEnumDatabaseMapper.getInstance();

            try
            {
                // Create and open connection.
                connection = new SqlConnection(connectionParam);
                connection.Open();

                // 2. Query to retrieve results.
                command = new SqlCommand("sp_get_next_unpaid_scheduled_payments", connection);
                command.CommandType = CommandType.StoredProcedure;

                idParam = command.Parameters.Add("@accessCode", SqlDbType.Int);
                idParam.Value = accessCode;
                
                if (log.IsDebugEnabled)
                {
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {
                        if (log.IsDebugEnabled)
                        {
                            log.Debug(string.Format("{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value));
                        }
                    }
                }

                // Retrieve data
                rs = command.ExecuteReader();

                while (rs.Read())
                {
                    string _billingType = (rs.IsDBNull(rs.GetOrdinal("billing_type"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("billing_type"))).Trim();
                    int _accessCode = (rs.IsDBNull(rs.GetOrdinal("access_code"))) ? (int)0 : (int)(rs.GetInt32(rs.GetOrdinal("access_code")));
                    string _payGroup = (rs.IsDBNull(rs.GetOrdinal("pay_group"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("pay_group"))).Trim();
                    int _payNum = (rs.IsDBNull(rs.GetOrdinal("pay_num"))) ? (int)0 : (int)(rs.GetInt32(rs.GetOrdinal("pay_num")));
                    DateTime _payDueDate = (rs.IsDBNull(rs.GetOrdinal("pay_due_date"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("pay_due_date")));
                    decimal _rateAmt = (rs.IsDBNull(rs.GetOrdinal("rate_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("rate_amt")));
                    decimal _initAmt = (rs.IsDBNull(rs.GetOrdinal("init_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init_amt")));
                    decimal _init2Amt = (rs.IsDBNull(rs.GetOrdinal("init2_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init2_amt")));
                    decimal _init3Amt = (rs.IsDBNull(rs.GetOrdinal("init3_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init3_amt")));
                    decimal _init4Amt = (rs.IsDBNull(rs.GetOrdinal("init4_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init4_amt")));
                    decimal _init5Amt = (rs.IsDBNull(rs.GetOrdinal("init5_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init5_amt")));
                    decimal _accrualAmt = (rs.IsDBNull(rs.GetOrdinal("accrual_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("accrual_amt")));
                    decimal _extraChargeAmt = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt")));
                    decimal _extraChargeAmt2 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt2"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt2")));
                    decimal _extraChargeAmt3 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt3"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt3")));
                    decimal _extraChargeAmt4 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt4"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt4")));
                    decimal _extraChargeAmt5 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt5"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt5")));
                    decimal _creditAmt = (rs.IsDBNull(rs.GetOrdinal("credit_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("credit_amt")));
                    decimal _paidAmt = (rs.IsDBNull(rs.GetOrdinal("paid_amount"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("paid_amount")));
                    DateTime _paidDate = (rs.IsDBNull(rs.GetOrdinal("paid_date"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("paid_date")));
                    DateTime _lastUpdated = (rs.IsDBNull(rs.GetOrdinal("last_updated"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("last_updated")));
                    string _discount_code = (rs.IsDBNull(rs.GetOrdinal("discount_code"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("discount_code"))).Trim();
                    short _fee_type = (rs.IsDBNull(rs.GetOrdinal("fee_type"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type")));
                    short _fee_type2 = (rs.IsDBNull(rs.GetOrdinal("fee_type2"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type2")));
                    short _fee_type3 = (rs.IsDBNull(rs.GetOrdinal("fee_type3"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type3")));
                    short _fee_type4 = (rs.IsDBNull(rs.GetOrdinal("fee_type4"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type4")));
                    short _fee_type5 = (rs.IsDBNull(rs.GetOrdinal("fee_type5"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type5")));

                    //Create new ScheduledPaymentVO object
                    vo = new ScheduledPaymentVO();
                    vo.AccessCode = _accessCode;
                    vo.AccrualAmt = _accrualAmt;
                    vo.BillingType = (PaymentBillingTypeEnum)paymentBillingTypeDbMapper.mapStringAsEnum(string.Format("{0}", _billingType));
                    vo.CreditAmt = _creditAmt;
                    vo.ExtraChargeAmt = _extraChargeAmt;
                    vo.ExtraChargeType = _fee_type;
                    vo.ExtraChargeAmt2 = _extraChargeAmt2;
                    vo.ExtraChargeType = _fee_type2;
                    vo.ExtraChargeAmt3 = _extraChargeAmt3;
                    vo.ExtraChargeType = _fee_type3;
                    vo.ExtraChargeAmt4 = _extraChargeAmt4;
                    vo.ExtraChargeType = _fee_type4;
                    vo.ExtraChargeAmt5 = _extraChargeAmt5;
                    vo.ExtraChargeType = _fee_type5;
                    vo.InitAmt = _initAmt;
                    vo.Init2Amt = _init2Amt;
                    vo.Init3Amt = _init3Amt;
                    vo.Init4Amt = _init4Amt;
                    vo.Init5Amt = _init5Amt;
                    vo.LastUpdated = _lastUpdated;
                    vo.PaidAmt = _paidAmt;
                    vo.PaidDate = _paidDate;
                    vo.PayDueDate = _payDueDate;
                    vo.PayGroup = _payGroup;
                    vo.PayNum = _payNum;
                    vo.RateAmt = _rateAmt;
                    vo.EffectiveDate = _payDueDate;
                    vo.DiscountCode = _discount_code;

                    // list is ordered by time asc, so we have to wait until last one
                }

                // Close reader.
                rs.Close();
            }
            catch (Exception e)
            {
                log.Error(e.ToString());
                throw e;
            }
            finally
            {

                if (connection != null)
                {
                    connection.Close();
                    connection = null;
                }
            }

            return vo;
        }
        public ScheduledPaymentVO retrieveExternalUpcomingSchedulePayment(int accessCode, string fac)
        {
            if (accessCode <= 0)
            {
                // Should throw an exception and log this.
                string msg = string.Format("accessCode [{0}] is not valid.", accessCode);
                log.Error(msg);
                throw new Exception(msg);
            }

            SqlConnection connection = null;
            SqlCommand command = null;
            SqlDataReader rs = null;
            SqlParameter idParam = null;
            ScheduledPaymentVO vo = null;

            // 1. Get the connection string.
            PaymentBillingTypeEnumDatabaseMapper paymentBillingTypeDbMapper = PaymentBillingTypeEnumDatabaseMapper.getInstance();
            ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();

            // 1. Get the connection string.
            string connectionParam = ShapeConnection.retrieveConnection(ConfigurationManager.AppSettings["SQLDBConnString"]);
            MembershipStatusEnumDatabaseMapper membershipStatusDbMapper = MembershipStatusEnumDatabaseMapper.getInstance();

            FacilityDAO fDao = new FacilityDAO();
            FacilityVO fVo = fDao.retrieveFacilityVO(fac);
            connectionParam = fVo.ConnectionString;

            try
            {
                // Create and open connection.
                connection = new SqlConnection(connectionParam);
                connection.Open();

                // 2. Query to retrieve results.
                command = new SqlCommand("sp_get_next_unpaid_scheduled_payments", connection);
                command.CommandType = CommandType.StoredProcedure;

                idParam = command.Parameters.Add("@accessCode", SqlDbType.Int);
                idParam.Value = accessCode;

                if (log.IsDebugEnabled)
                {
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {
                        if (log.IsDebugEnabled)
                        {
                            log.Debug(string.Format("{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value));
                        }
                    }
                }

                // Retrieve data
                rs = command.ExecuteReader();

                while (rs.Read())
                {
                    string _billingType = (rs.IsDBNull(rs.GetOrdinal("billing_type"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("billing_type"))).Trim();
                    int _accessCode = (rs.IsDBNull(rs.GetOrdinal("access_code"))) ? (int)0 : (int)(rs.GetInt32(rs.GetOrdinal("access_code")));
                    string _payGroup = (rs.IsDBNull(rs.GetOrdinal("pay_group"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("pay_group"))).Trim();
                    int _payNum = (rs.IsDBNull(rs.GetOrdinal("pay_num"))) ? (int)0 : (int)(rs.GetInt32(rs.GetOrdinal("pay_num")));
                    DateTime _payDueDate = (rs.IsDBNull(rs.GetOrdinal("pay_due_date"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("pay_due_date")));
                    decimal _rateAmt = (rs.IsDBNull(rs.GetOrdinal("rate_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("rate_amt")));
                    decimal _initAmt = (rs.IsDBNull(rs.GetOrdinal("init_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init_amt")));
                    decimal _init2Amt = (rs.IsDBNull(rs.GetOrdinal("init2_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init2_amt")));
                    decimal _init3Amt = (rs.IsDBNull(rs.GetOrdinal("init3_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init3_amt")));
                    decimal _init4Amt = (rs.IsDBNull(rs.GetOrdinal("init4_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init4_amt")));
                    decimal _init5Amt = (rs.IsDBNull(rs.GetOrdinal("init5_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("init5_amt")));
                    decimal _accrualAmt = (rs.IsDBNull(rs.GetOrdinal("accrual_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("accrual_amt")));
                    decimal _extraChargeAmt = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt")));
                    decimal _extraChargeAmt2 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt2"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt2")));
                    decimal _extraChargeAmt3 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt3"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt3")));
                    decimal _extraChargeAmt4 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt4"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt4")));
                    decimal _extraChargeAmt5 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt5"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("extra_charge_amt5")));
                    decimal _creditAmt = (rs.IsDBNull(rs.GetOrdinal("credit_amt"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("credit_amt")));
                    decimal _paidAmt = (rs.IsDBNull(rs.GetOrdinal("paid_amount"))) ? (decimal)0.00 : (decimal)(rs.GetDecimal(rs.GetOrdinal("paid_amount")));
                    DateTime _paidDate = (rs.IsDBNull(rs.GetOrdinal("paid_date"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("paid_date")));
                    DateTime _lastUpdated = (rs.IsDBNull(rs.GetOrdinal("last_updated"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("last_updated")));
                    string _discount_code = (rs.IsDBNull(rs.GetOrdinal("discount_code"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("discount_code"))).Trim();
                    short _fee_type = (rs.IsDBNull(rs.GetOrdinal("fee_type"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type")));
                    short _fee_type2 = (rs.IsDBNull(rs.GetOrdinal("fee_type2"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type2")));
                    short _fee_type3 = (rs.IsDBNull(rs.GetOrdinal("fee_type3"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type3")));
                    short _fee_type4 = (rs.IsDBNull(rs.GetOrdinal("fee_type4"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type4")));
                    short _fee_type5 = (rs.IsDBNull(rs.GetOrdinal("fee_type5"))) ? (short)0 : (short)(rs.GetInt16(rs.GetOrdinal("fee_type5")));

                    //Create new ScheduledPaymentVO object
                    vo = new ScheduledPaymentVO();
                    vo.AccessCode = _accessCode;
                    vo.AccrualAmt = _accrualAmt;
                    vo.BillingType = (PaymentBillingTypeEnum)paymentBillingTypeDbMapper.mapStringAsEnum(string.Format("{0}", _billingType));
                    vo.CreditAmt = _creditAmt;
                    vo.ExtraChargeAmt = _extraChargeAmt;
                    vo.ExtraChargeType = _fee_type;
                    vo.ExtraChargeAmt2 = _extraChargeAmt2;
                    vo.ExtraChargeType = _fee_type2;
                    vo.ExtraChargeAmt3 = _extraChargeAmt3;
                    vo.ExtraChargeType = _fee_type3;
                    vo.ExtraChargeAmt4 = _extraChargeAmt4;
                    vo.ExtraChargeType = _fee_type4;
                    vo.ExtraChargeAmt5 = _extraChargeAmt5;
                    vo.ExtraChargeType = _fee_type5;
                    vo.InitAmt = _initAmt;
                    vo.Init2Amt = _init2Amt;
                    vo.Init3Amt = _init3Amt;
                    vo.Init4Amt = _init4Amt;
                    vo.Init5Amt = _init5Amt;
                    vo.LastUpdated = _lastUpdated;
                    vo.PaidAmt = _paidAmt;
                    vo.PaidDate = _paidDate;
                    vo.PayDueDate = _payDueDate;
                    vo.PayGroup = _payGroup;
                    vo.PayNum = _payNum;
                    vo.RateAmt = _rateAmt;
                    vo.EffectiveDate = _payDueDate;
                    vo.DiscountCode = _discount_code;

                    // list is ordered by time asc, so we have to wait until last one
                }

                // Close reader.
                rs.Close();
            }
            catch (Exception e)
            {
                log.Error(e.ToString());
                throw e;
            }
            finally
            {

                if (connection != null)
                {
                    connection.Close();
                    connection = null;
                }
            }

            return vo;
        }
        public ArrayList retrieveOpenBalancesMembershipsPackagesList( MembershipStatusEnum membershipStatus, bool filler )
		{
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("About to retrieve the open balance list for memberships." ) );
			}		
									
			Hashtable keyList = new Hashtable();
			ArrayList list = new ArrayList();
			ArrayList summaryList = new ArrayList();
			SqlConnection connection = null;
            SqlTransaction transaction = null;
            SqlCommand command = null;
			SqlDataReader rsFirst = null;		
			SqlParameter idParam = null;
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			MembershipStatusEnumDatabaseMapper membershipStatusDbMapper = MembershipStatusEnumDatabaseMapper.getInstance();
			
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();
				transaction = connection.BeginTransaction();

                // 2. Query to retrieve the user for additional
                command = new SqlCommand( "sp_get_open_balances_memberships_key", connection );
				command.CommandType = CommandType.StoredProcedure;										
																
				idParam = command.Parameters.Add("@membershipStatus", SqlDbType.NVarChar, 1 );
				if( membershipStatus == 0x0000 )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = membershipStatusDbMapper.mapEnumAsString( membershipStatus );
				}					
				
				// Retrieve data
				rsFirst = command.ExecuteReader();			
								
				while( rsFirst.Read() )
				{									
					string _userid = ( rsFirst.IsDBNull( rsFirst.GetOrdinal("cid") ) ) ? string.Empty : ( rsFirst.GetString( rsFirst.GetOrdinal("cid") ) ).Trim();
					int _accessCode = ( rsFirst.IsDBNull( rsFirst.GetOrdinal("access_code") ) ) ? 0 : ( rsFirst.GetInt32( rsFirst.GetOrdinal("access_code") ) );
					keyList.Add( _userid, _accessCode );
				}
								
				// Close reader.
				rsFirst.Close();		
				
				SortedList sl = new SortedList( keyList );
				foreach ( DictionaryEntry myDE in sl )
				{
					string cid = (string)myDE.Key;
					int accessCode = (int)myDE.Value;
			    	list.AddRange( retrieveOpenMembershipBalanceForMember( accessCode, cid, connection, transaction) );	
				}

				transaction.Commit();

                connection.Close();

			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw new Exception();
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
											
			return list;		
		}


        public ArrayList retrieveExternalOpenMembershipBalanceForMember(string cid, string fac)
        {

            // Get AccessCode
            int accessCode = 0;

            try
            {
                MemberDAO mebDao = new MemberDAO();
                MemberVO mebVo = mebDao.retrieveMemberVO(cid, fac);
                if (mebVo != null)
                {
                    accessCode = mebVo.AccessCode;
                }
            }
            catch (Exception e)
            {
                string msg = e.Message;
                GuestDAO gDao = new GuestDAO();
                GuestVO gVo = gDao.retrieveExternalGuestVO(cid, fac);
                if (gVo != null)
                {
                    accessCode = gVo.AccessCode;
                }
            }

            ArrayList list = retrieveExternalMembershipOpenBalancesList(accessCode, null, 0x0000, fac);

            /*SqlConnection connection = null;
			
            // 1. Get the connection string.
            string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			
            try
            {
                // Create and open connection.
                connection = new SqlConnection( connectionParam );
                connection.Open();
	    		    	
                list = retrieveOpenMembershipBalanceForMember( accessCode, cid, connection );
	    		
                connection.Close();

            }
            catch( Exception e )
            {
                log.Error( e.ToString() );
                throw new Exception();
            }
            finally
            {
			
                if ( connection != null )			
                {
                    connection.Close();
                    connection = null;
                }			
            }*/

            return list;

        }
	

	    public ArrayList retrieveOpenMembershipBalanceForMember( string cid )
	    {
	    	
			// Get AccessCode
			int accessCode = 0;

			try
			{
				MemberDAO mebDao = new MemberDAO();
				MemberVO mebVo = mebDao.retrieveMemberVO( cid );			
				if( mebVo != null )
				{
					accessCode = mebVo.AccessCode;
				}	
			}
			catch( Exception e )
			{
				string msg = e.Message;
				GuestDAO gDao = new GuestDAO();
				GuestVO gVo = gDao.retrieveGuestVO( cid );
				if( gVo != null )
				{
					accessCode = gVo.AccessCode;
				}	
			}					    	
	    	
			ArrayList list = retrieveMembershipOpenBalancesList( accessCode, null, 0x0000 );
	    	
	    	/*SqlConnection connection = null;
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();
	    		    	
	    		list = retrieveOpenMembershipBalanceForMember( accessCode, cid, connection );
	    		
				connection.Close();

			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw new Exception();
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}*/
			
			return list;
	    		
	    }
		
	    public MembershipOpenBalancesVO retrieveOpenMembershipBalanceForMember( int accessCode )
	    {	    		    	    		    	
			if( accessCode <= 0 )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "accessCode is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("accessCode is [{0}]", accessCode) );
			}
	    		    	
			MembershipOpenBalancesVO vo = null;
			ArrayList list = retrieveMembershipOpenBalancesList( accessCode, null, 0x0000 );	    				
			
			// Check that only one MembershipOpenBalancesVO object was created.
			if( list.Count > 1 )
			{
				log.Error( "More than one MembershipOpenBalancesVO was retrieved." );

				// Temporarily clear list.
				list = new ArrayList();
			}
			
			if( list.Count == 1 )
			{
				if( log.IsDebugEnabled )
				{
					log.Debug("One MembershipOpenBalancesVO retrieved from query and returned.");						
				}
				vo = ( MembershipOpenBalancesVO )list[0];
				if(log.IsDebugEnabled)
				{
					log.Debug( string.Format( "Retrieved MembershipOpenBalancesVO [{0}].", vo ) );
				}
			}
			
			return vo;    		
	    }	    
	    
	    public MembershipOpenBalancesVO retrieveOpenMembershipBalanceForMember( int accessCode, DateTime effDate )
	    {	    		    	    		    	
			if( accessCode <= 0 )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "accessCode is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("accessCode is [{0}]", accessCode) );
			}
	    		    	
			if( effDate.CompareTo( DateTime.MinValue ) <= 0 )
			{
				effDate = DateAndTimeUtil.getLocaleDateTime().Date;
			}			
			
			MembershipOpenBalancesVO vo = null;
			ArrayList list = retrieveMembershipOpenBalancesList( accessCode, null, 0x0000, effDate );	    				
			
			// Check that only one MembershipOpenBalancesVO object was created.
			if( list.Count > 1 )
			{
				log.Error( "More than one MembershipOpenBalancesVO was retrieved." );

				// Temporarily clear list.
				list = new ArrayList();
			}
			
			if( list.Count == 1 )
			{
				if( log.IsDebugEnabled )
				{
					log.Debug("One MembershipOpenBalancesVO retrieved from query and returned.");						
				}
				vo = ( MembershipOpenBalancesVO )list[0];
				if(log.IsDebugEnabled)
				{
					log.Debug( string.Format( "Retrieved MembershipOpenBalancesVO [{0}].", vo ) );
				}
			}
			
			return vo;    		
	    }	    
	    
		public ArrayList retrieveOpenMembershipBalanceForMember( int accessCode, string cid, SqlConnection connection, SqlTransaction transaction )
		{
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("About to retrieve the open balance for member [{0}].", cid ) );
			}											
				
			ArrayList paymentList = new ArrayList();
			ArrayList list = new ArrayList();

			SqlCommand command = null;
			SqlDataReader rs = null;				
			
			command = new SqlCommand( "sp_get_payments", connection, transaction);
			command.CommandType = CommandType.StoredProcedure;										
													
			SqlParameter sqlParam = null;
			
			sqlParam = command.Parameters.Add( "@billing_type", SqlDbType.NVarChar, 1 );
			sqlParam.Value = 'M';
			
			sqlParam = command.Parameters.Add( "@access_code", SqlDbType.Int );
			sqlParam.Value = accessCode;			
			
			sqlParam = command.Parameters.Add( "@pay_group", SqlDbType.NVarChar, 50 );
			sqlParam.Value = cid;			
			
			if( log.IsDebugEnabled )
			{
			   for (int i = 0; i < command.Parameters.Count; i++) 
			   {
			   	log.Debug( string.Format( "{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value ) );
			   }
			}			
			
			// Retrieve data
			rs = command.ExecuteReader();			
							
			while( rs.Read() )
			{				
				
				string _billingType = ( rs.IsDBNull( rs.GetOrdinal("billing_type") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("billing_type") ) ).Trim();				
				int _accessCode = ( rs.IsDBNull( rs.GetOrdinal("access_code") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("access_code") ) );
				string _payGroup = ( rs.IsDBNull( rs.GetOrdinal("pay_group") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("pay_group") ) ).Trim();
				int _payNum = ( rs.IsDBNull( rs.GetOrdinal("pay_num") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("pay_num") ) );				
				DateTime _payDueDate = ( rs.IsDBNull( rs.GetOrdinal("pay_due_date") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("pay_due_date") ) );
				decimal _rateAmt = ( rs.IsDBNull( rs.GetOrdinal("rate_amt") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("rate_amt") ) );
				decimal _initAmt = ( rs.IsDBNull( rs.GetOrdinal("init_amt") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("init_amt") ) );
                decimal _init2Amt = (rs.IsDBNull(rs.GetOrdinal("init2_amt"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("init2_amt")));
                decimal _init3Amt = (rs.IsDBNull(rs.GetOrdinal("init3_amt"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("init3_amt")));
                decimal _init4Amt = (rs.IsDBNull(rs.GetOrdinal("init4_amt"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("init4_amt")));
                decimal _init5Amt = (rs.IsDBNull(rs.GetOrdinal("init5_amt"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("init5_amt")));
                decimal _accrualAmt = (rs.IsDBNull(rs.GetOrdinal("accrual_amt"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("accrual_amt")));
				decimal _extraChargeAmt = ( rs.IsDBNull( rs.GetOrdinal("extra_charge_amt") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("extra_charge_amt") ) );
                decimal _extraChargeAmt2 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt2"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("extra_charge_amt2")));
                decimal _extraChargeAmt3 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt3"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("extra_charge_amt3")));
                decimal _extraChargeAmt4 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt4"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("extra_charge_amt4")));
                decimal _extraChargeAmt5 = (rs.IsDBNull(rs.GetOrdinal("extra_charge_amt5"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("extra_charge_amt5")));
                decimal _creditAmt = (rs.IsDBNull(rs.GetOrdinal("credit_amt"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("credit_amt")));
				decimal _paidAmount = ( rs.IsDBNull( rs.GetOrdinal("paid_amount") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("paid_amount") ) );
				DateTime _paidDate = ( rs.IsDBNull( rs.GetOrdinal("paid_date") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("paid_date") ) );
				DateTime _lastUpdated = ( rs.IsDBNull( rs.GetOrdinal("last_updated") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("last_updated") ) );
				
				//Create new PaymentVO object
				PaymentVO vo = new PaymentVO();
				vo.AccessCode = _accessCode;
				vo.AccrualAmount = _accrualAmt;
				vo.BillingType = _billingType;
				vo.CreditAmt = _creditAmt;
                vo.ExtraChargeAmount = _extraChargeAmt;
                vo.ExtraChargeAmount2 = _extraChargeAmt2;
                vo.ExtraChargeAmount3 = _extraChargeAmt3;
                vo.ExtraChargeAmount4 = _extraChargeAmt4;
                vo.ExtraChargeAmount5 = _extraChargeAmt5;
                vo.InitAmount = _initAmt;
                vo.Init2Amount = _init2Amt;
                vo.Init3Amount = _init3Amt;
                vo.Init4Amount = _init4Amt;
                vo.Init5Amount = _init5Amt;
				vo.LastUpdated = _lastUpdated;
				vo.PaidAmount = _paidAmount;
				vo.PaidDate = _paidDate;
				vo.PaymentDueDate = _payDueDate;
				vo.PaymentGroup = _payGroup;
				vo.PaymentNumber = _payNum;
				vo.RateAmount = _rateAmt;
				
				// Add to ArrayList as a validation check
				paymentList.Add( vo );
			}			
									
			// Close reader.
			rs.Close();		
								
			if( paymentList.Count > 0 )
			{				
				decimal amountExpected = (decimal)0;
				decimal amountPaid = (decimal)0;
				decimal amountDue = (decimal)0;
				
				DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();
				DateTime todaysDate = new DateTime( rightNow.Year, rightNow.Month, rightNow.Day );
							
				foreach( PaymentVO pv in paymentList )
				{
					log.Debug( pv );
					if( pv.PaymentDueDate.CompareTo( todaysDate ) <= 0 )
					{
                        amountExpected = amountExpected + (pv.RateAmount + pv.AccrualAmount + pv.CreditAmt + pv.ExtraChargeAmount + pv.ExtraChargeAmount2 + pv.ExtraChargeAmount3 + pv.ExtraChargeAmount4 + pv.ExtraChargeAmount5 + pv.InitAmount + pv.Init2Amount + pv.Init3Amount + pv.Init4Amount + pv.Init5Amount);
						amountPaid = amountPaid + pv.PaidAmount;
						amountDue = amountExpected - amountPaid;																								
						
						log.Debug( string.Format( "amountExpected: {0:C}", amountExpected ) );
						log.Debug( string.Format( "amountPaid: {0:C}", amountPaid ) );
						log.Debug( string.Format( "amountDue: {0:C}", amountDue ) );
					}														
				}											
					 							
				MembershipOpenBalancesVO vo = new MembershipOpenBalancesVO( cid );
				vo.AccessCode = accessCode;
				vo.ExpectedPayments = amountExpected;
				vo.Payments = amountPaid;
				vo.AmountDue = amountDue;
				
				//vo.Balance = (decimal)0.00;
				//vo.ExpectedBalance = (decimal)0.00;					
				//vo.Accrual = (decimal)0.00;
				//vo.Term = string.Empty;
				//vo.StartDate = startDate;
				//vo.EndDate = endDate;
				//vo.NeedFirstPayment = needFirstPayment;
				//vo.InitFee = initFee;
				//vo.SalesTax = salesTax;
				//vo.PayFreq = payFreq;
				//vo.EndBillDate = endDate;
				//vo.FreezeDate = _freezeDate;
				//vo.MonthlyRate = lastPeriodRate;
				//vo.Price = (decimal)0.00;
				//vo.BillStartDate = currentBillDate;
				//vo.DailyRateAdjustment = lastRate - dailyRateRangeBuffer;
				//vo.FreezeDays = _freezeDays;
				//vo.FreezeCount = _freezeCount;
				//vo.LastPaymentDate = _lastPaymentDate;									
				//vo.ExtraCharges = extraCharges;
				
				// Add to ArrayList as a validation check
				list.Add( vo );									
				
			}
											
			return list;					
		}			
		
		public ArrayList retrieveMembershipDailyMoneyList( DateTime minRecordDate, DateTime maxRecordDate )
		{
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("About to retrieve the membership daily money report for minRecordDate [{0}], maxRecordDate [{1}]", minRecordDate, maxRecordDate  ) );
			}		
			
			ArrayList list = new ArrayList();
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;					
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();

				// 2. Query to retrieve the user.
				command = new SqlCommand( "sp_rpt_membership_daily_money", connection );
				command.CommandType = CommandType.StoredProcedure;							
				
				SqlParameter sqlParam = null;
												
				sqlParam = command.Parameters.Add( "@minRecordDate", SqlDbType.DateTime );
				sqlParam.Value = minRecordDate;
				if( minRecordDate.Year == 1900 )
				{
					sqlParam.Value = DBNull.Value;
				}		
				
				sqlParam = command.Parameters.Add( "@maxRecordDate", SqlDbType.DateTime );
				sqlParam.Value = new DateTime( maxRecordDate.Year, maxRecordDate.Month, maxRecordDate.Day, 23, 59, 59 );
				if( maxRecordDate.Year == 1900 )
				{
					sqlParam.Value = DBNull.Value;
				}					
								
				// Retrieve data
				rs = command.ExecuteReader();			
								
				while( rs.Read() )
				{

					DateTime _entDate = ( rs.IsDBNull( rs.GetOrdinal("entDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("entDate") ) );
					string _orderId = ( rs.IsDBNull( rs.GetOrdinal("orderId") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("orderId") ) ).Trim();
					Decimal _amount = ( rs.IsDBNull( rs.GetOrdinal("amount") ) ) ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amount") ) );
					string _userId = ( rs.IsDBNull( rs.GetOrdinal("cid") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("cid") ) ).Trim();
					string _reason = ( rs.IsDBNull( rs.GetOrdinal("reason") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("reason") ) ).Trim();
					string _payType = ( rs.IsDBNull( rs.GetOrdinal("payType") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("payType") ) ).Trim();
					string _packageType = ( rs.IsDBNull( rs.GetOrdinal("package") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("package") ) ).Trim();
					string _payDescription = ( rs.IsDBNull( rs.GetOrdinal("description") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("description") ) ).Trim();
										
					MoneyTransactionsVO vo = new MoneyTransactionsVO( _entDate, _orderId, _reason, _userId, _payType, _packageType, _payDescription, _amount );				
					
					// Add to ArrayList as a validation check
					list.Add( vo );				
				}
				
				// Close reader.
				rs.Close();								
				connection.Close();

			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw new Exception();
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
			
			return list;		
		}		
		
		public ArrayList retrieveMFTrainingReceivablesSummaryList( MFTrainingReceivablesSummarySearchVO searchCriteria )
		{			
			
			if( searchCriteria == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "MFTrainingReceivablesSummarySearchVO [{0}] is null.", searchCriteria );
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("MFTrainingReceivablesSummarySearchVO is [{0}]", searchCriteria) );
			}
			
			FacilityListSearchVO fsc = new FacilityListSearchVO();
			fsc.GroupCode = searchCriteria.GroupCode;
			fsc.IncludeInMultifacility = "true";
			
			FacilityDAO facDao = new FacilityDAO();
			ArrayList facilities = facDao.retrieveFacilities( fsc );
			
			ArrayList list = new ArrayList();
			
			IEnumerator de = facilities.GetEnumerator();
			while ( de.MoveNext() )
			{
				FacilityVO fac = (FacilityVO)de.Current;
								
				ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();
				
				string connectionParam = ShapeConnection.retrieveConnection( fac.ConnectionString );
				
				ArrayList dbList = new ArrayList();
				
				try
				{
					dbList = this.retrieveMFTrainingReceivablesSummaryList( fac, searchCriteria );
				}
				catch( Exception e )
				{
					log.Error( e.ToString() );
					throw e;
				}
				
				if( dbList.Count > 0 )
				{
					list.AddRange( dbList );
				}
			}
			
			return list;		
		}		

		public ArrayList retrieveMFTrainingReceivablesSummaryList( FacilityVO fac, MFTrainingReceivablesSummarySearchVO searchCriteria  )
		{			
			if( searchCriteria == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "MFTrainingReceivablesSummarySearchVO [{0}] is null.", searchCriteria );
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( fac == null )
			{
				string msg = string.Format( "FacilityVO [{0}] is null.", fac );
				log.Error( msg );
				throw new Exception( msg );
			}		

			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("MFTrainingReceivablesSummarySearchVO is [{0}]", searchCriteria) );
			}
			
			ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();
			
			string connectionParam = ShapeConnection.retrieveConnection( fac.ConnectionString );
			
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;					
			ArrayList list = new ArrayList();

            string sqltimeoutstr = ShapeConfiguration.AppSettings("SQLTimeOut");
            int sqltimeout = 180;
            if (!Int32.TryParse(sqltimeoutstr, out sqltimeout))
            {
                sqltimeout = 180;
            } 
            try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();
			
				// 2. Query to retrieve the user.
				command = new SqlCommand( "sp_rpt_training_money_transactions", connection );
                command.CommandTimeout = sqltimeout;
				command.CommandType = CommandType.StoredProcedure;							
				
				SqlParameter idParam = null;
				
				idParam = command.Parameters.Add("@effDate", SqlDbType.DateTime );
				if( searchCriteria.EffDate == null || searchCriteria.EffDate.Item == null )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = (DateTime)(searchCriteria.EffDate.Item);
				}
			
				idParam = command.Parameters.Add("@effDateOperator", SqlDbType.NVarChar, 2 );
				if( searchCriteria.EffDate == null || searchCriteria.EffDate.Operator == 0x0000 )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = comparisonOperatorDbMapper.mapEnumAsString( searchCriteria.EffDate.Operator );
				}				
				
				idParam = command.Parameters.Add("@effDate2", SqlDbType.DateTime );
				if( searchCriteria.EffDate == null || searchCriteria.EffDate.Item2 == null )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = (DateTime)(searchCriteria.EffDate.Item2);
				}									
													
				if (log.IsDebugEnabled)
				{
					for (int i = 0; i < command.Parameters.Count; i++)
					{                    	
						log.Debug(string.Format("{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value));
					}
				}					
									
				// Retrieve data
				rs = command.ExecuteReader();			
									
				MFTrainingReceivablesSummaryVO vo = new MFTrainingReceivablesSummaryVO();
				vo.Facility = fac;
				
				while( rs.Read() )
				{
					double _amount = ( rs.IsDBNull( rs.GetOrdinal("amount") ) ) ? (double)0.00 : ( rs.GetDouble( rs.GetOrdinal("amount") ) );
					int _itemCount = ( rs.IsDBNull( rs.GetOrdinal("itemCount") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("itemCount") ) );
					string _payType = ( rs.IsDBNull( rs.GetOrdinal("payType") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("payType") ) ).Trim();						
					
					switch( _payType )
					{
						case "CC":
							vo.CreditAmount = (decimal)_amount;
							vo.CreditCount = _itemCount;
							break;
							
						case "CA":
							vo.CashAmount = (decimal)_amount;
							vo.CashCount = _itemCount;
							break;
							
						case "BA":
							vo.PaperCheckAmount = (decimal)_amount;
							vo.PaperCheckCount = _itemCount;
							break;
							
						case "EC":
							vo.ECheckAmount = (decimal)_amount;
							vo.ECheckCount = _itemCount;
							break;								
					}																	
			
				}
			
				// Add to ArrayList as a validation check
				list.Add( vo );					
				
				// Close reader.
				rs.Close();								
				connection.Close();
			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw new Exception();
			}
			finally
			{
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
			
			return list;		
		}							

		public ArrayList retrieveMembershipOpenBalancesList( int accessCode, string cid, MembershipStatusEnum membershipStatus, DateTime effDate  )
		{		
			OpenBalanceMembershipListSearchVO searchCriteria = new OpenBalanceMembershipListSearchVO();
			searchCriteria.AccessCode = accessCode;
			searchCriteria.Cid = cid;
			searchCriteria.EffectiveDate = effDate;
			
			return this.retrieveMembershipOpenBalancesList( searchCriteria );		
		}
		
		public ArrayList retrieveMembershipOpenBalancesList( int accessCode, string cid, MembershipStatusEnum membershipStatus  )
		{	
			OpenBalanceMembershipListSearchVO searchCriteria = new OpenBalanceMembershipListSearchVO();
			searchCriteria.AccessCode = accessCode;
			searchCriteria.Cid = cid;
			
			return this.retrieveMembershipOpenBalancesList( searchCriteria );
		}

        public ArrayList retrieveExternalMembershipOpenBalancesList(int accessCode, string cid, MembershipStatusEnum membershipStatus, string fac)
        {
            OpenBalanceMembershipListSearchVO searchCriteria = new OpenBalanceMembershipListSearchVO();
            searchCriteria.AccessCode = accessCode;
            searchCriteria.Cid = cid;

            return this.retrieveExternalMembershipOpenBalancesList(searchCriteria, fac);
        }
			
		public MembershipOpenBalancesVO retrieveMembershipOpenBalance( int accessCode, string cid, MembershipStatusEnum membershipStatus  )
		{	
			if( accessCode <= 0 )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "accessCode is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}
			
			if( cid == null || string.Empty.Equals( cid.Trim() ) )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "cid is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}			
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("accessCode is [{0}]", accessCode) );
			}
	    		    	
			MembershipOpenBalancesVO vo = null;
			OpenBalanceMembershipListSearchVO searchCriteria = new OpenBalanceMembershipListSearchVO();
			searchCriteria.AccessCode = accessCode;
			searchCriteria.Cid = cid;									
			
			ArrayList list = retrieveMembershipOpenBalancesList( searchCriteria );
			
			// Check that only one MembershipOpenBalancesVO object was created - should only be one for pay_group
			if( list.Count > 1 )
			{
				log.Error( "More than one MembershipOpenBalancesVO was retrieved." );

				// Temporarily clear list.
				list = new ArrayList();
			}
			
			if( list.Count == 1 )
			{
				if( log.IsDebugEnabled )
				{
					log.Debug("One MembershipOpenBalancesVO retrieved from query and returned.");						
				}
				vo = ( MembershipOpenBalancesVO )list[0];
				if(log.IsDebugEnabled)
				{
					log.Debug( string.Format( "Retrieved MembershipOpenBalancesVO [{0}].", vo ) );
				}
			}
			
			return vo;  
		}


        public ArrayList retrieveExternalMembershipOpenBalancesList(OpenBalanceMembershipListSearchVO searchCriteria, string fac)
        {

            if (searchCriteria == null)
            {
                // Should throw an exception and log this.
                string msg = string.Format("OpenBalanceMembershipListSearchVO [{0}] is null.", searchCriteria);
                log.Error(msg);
                throw new Exception(msg);
            }

            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("OpenBalanceMembershipListSearchVO is [{0}]", searchCriteria));
            }

            SqlConnection connection = null;
            SqlCommand command = null;
            SqlDataReader rs = null;
            SqlParameter idParam = null;
            ArrayList list = new ArrayList();

            // 1. Get the connection string.
            string connectionParam = ShapeConnection.retrieveConnection(ConfigurationManager.AppSettings["SQLDBConnString"]);
            MembershipPayFrequencyEnumDatabaseMapper payFrequencyDbMapper = MembershipPayFrequencyEnumDatabaseMapper.getInstance();
            MembershipStatusEnumDatabaseMapper membershipStatusDbMapper = MembershipStatusEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();
            UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
            UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
            InvoicePreferenceEnumDatabaseMapper invoicePreferenceEnumDbMapper = InvoicePreferenceEnumDatabaseMapper.getInstance();
            MembershipSubtypeEnumDatabaseMapper membershipSubtypeDbMapper = MembershipSubtypeEnumDatabaseMapper.getInstance();
            EFTPreferenceEnumDatabaseMapper eftPrefDbMapper = EFTPreferenceEnumDatabaseMapper.getInstance();

            DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();

            FacilityDAO fDao = new FacilityDAO();
            FacilityVO fVo = fDao.retrieveFacilityVO(fac);
            connectionParam = fVo.ConnectionString;

            string sqltimeoutstr = ShapeConfiguration.AppSettings("SQLTimeOut");
            int sqltimeout = 240;
            if (!Int32.TryParse(sqltimeoutstr, out sqltimeout))
            {
                sqltimeout = 240;
            } 
            try
            {
                // Create and open connection.
                connection = new SqlConnection(connectionParam);
                connection.Open();

                // 2. Query to retrieve results.
                command = new SqlCommand("sp_get_membership_open_balance", connection);
                command.CommandTimeout = sqltimeout;
                command.CommandType = CommandType.StoredProcedure;

                idParam = command.Parameters.Add("@effectiveDate", SqlDbType.SmallDateTime);
                idParam.Value = string.Format("{0:MM/dd/yyyy}", searchCriteria.EffectiveDate);

                idParam = command.Parameters.Add("@billing_type", SqlDbType.NVarChar, 1);
                idParam.Value = "M";

                idParam = command.Parameters.Add("@access_code", SqlDbType.Int);
                idParam.Value = searchCriteria.AccessCode;
                if (searchCriteria.AccessCode <= 0)
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@pay_group", SqlDbType.NVarChar, 50);
                idParam.Value = DBNull.Value;

                idParam = command.Parameters.Add("@membershipStatus", SqlDbType.NVarChar, 1);
                if (searchCriteria.MembershipStatus == 0x0000)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = membershipStatusDbMapper.mapEnumAsString(searchCriteria.MembershipStatus);
                }

                idParam = command.Parameters.Add("@amountDueAmt", SqlDbType.Real);
                idParam.Value = DBNull.Value;
                if (searchCriteria.AmountDueAmt.CompareTo((decimal)0.00) > 0)
                {
                    idParam.Value = searchCriteria.AmountDueAmt;
                }

                idParam = command.Parameters.Add("@daysPastDue", SqlDbType.Int);
                idParam.Value = DBNull.Value;
                if (searchCriteria.DaysPastDue > -1)
                {
                    idParam.Value = searchCriteria.DaysPastDue;
                }

                idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40);
                idParam.Value = DBNull.Value;
                if (searchCriteria.Cid != null && !string.Empty.Equals(searchCriteria.Cid.Trim()))
                {
                    idParam.Value = searchCriteria.Cid;
                }

                idParam = command.Parameters.Add("@uid_operator", SqlDbType.NVarChar, 2);
                idParam.Value = DBNull.Value;
                if (searchCriteria.CidOperator != null && !string.Empty.Equals(searchCriteria.CidOperator.Trim()))
                {
                    idParam.Value = searchCriteria.CidOperator;
                }

                idParam = command.Parameters.Add("@user_status", SqlDbType.NVarChar, 1);
                idParam.Value = DBNull.Value;
                if (searchCriteria.UserStatus != 0x0000)
                {
                    idParam.Value = userStatusDbMapper.mapEnumAsString(searchCriteria.UserStatus);
                }

                idParam = command.Parameters.Add("@member_types", SqlDbType.NVarChar, 255);
                idParam.Value = DBNull.Value;
                if (!string.Empty.Equals(searchCriteria.MemberTypesDelimitedString))
                {
                    idParam.Value = searchCriteria.MemberTypesDelimitedString;
                }

                idParam = command.Parameters.Add("@membership_types", SqlDbType.NVarChar, -1);
                idParam.Value = DBNull.Value;
                if (!string.Empty.Equals(searchCriteria.MembershipTypesDelimitedString))
                {
                    idParam.Value = searchCriteria.MembershipTypesDelimitedString;
                }

                idParam = command.Parameters.Add("@user_group", SqlDbType.NVarChar, 1);
                idParam.Value = DBNull.Value;
                if (searchCriteria.UserGroup != 0x0000)
                {
                    idParam.Value = userTypeDbMapper.mapEnumAsString(searchCriteria.UserGroup);
                }

                idParam = command.Parameters.Add("@user_status_active_and_inactive", SqlDbType.Bit);
                idParam.Value = searchCriteria.UserStatusActiveAndInactive;

                idParam = command.Parameters.Add("@countAddons", SqlDbType.Bit);
                idParam.Value = searchCriteria.CountAddOns;

                if (log.IsDebugEnabled)
                {
                    string sql_proc_str = string.Format("[{0}]", command.CommandText);
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {
                        sql_proc_str = string.Format("{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals(command.Parameters[i].Value) ? "NULL" : string.Format("'{0}'", command.Parameters[i].Value));
                    }

                    log.Debug(sql_proc_str);
                }

                // Retrieve data
                rs = command.ExecuteReader();

                while (rs.Read())
                {
                    //billing_type varchar(1), access_code int, pay_group varchar(50), uid varchar(40), notes varchar(512), amountExpected decimal, 
                    //amountPaid decimal, amountDue decimal, term char(10) null, payFreq char(1) null, periodRate smallmoney null, lastPaidDate smalldatetime null )'
                    int _accessCode = (rs.IsDBNull(rs.GetOrdinal("access_code"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("access_code")));
                    string _userStatus = (rs.IsDBNull(rs.GetOrdinal("user_status"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("user_status"))).Trim();
                    string _userGroup = (rs.IsDBNull(rs.GetOrdinal("userGroup"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userGroup"))).Trim();
                    string _billing_type = (rs.IsDBNull(rs.GetOrdinal("billing_type"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("billing_type"))).Trim();
                    string _pay_group = (rs.IsDBNull(rs.GetOrdinal("pay_group"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("pay_group"))).Trim();
                    string _uid = (rs.IsDBNull(rs.GetOrdinal("uid"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("uid"))).Trim();
                    string _notes = (rs.IsDBNull(rs.GetOrdinal("notes"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("notes"))).Trim();
                    decimal _amountExpected = (rs.IsDBNull(rs.GetOrdinal("amountExpected"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amountExpected")));
                    decimal _amountPaid = (rs.IsDBNull(rs.GetOrdinal("amountPaid"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amountPaid")));
                    decimal _amountDue = (rs.IsDBNull(rs.GetOrdinal("amountDue"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amountDue")));
                    decimal _lateFee = (rs.IsDBNull(rs.GetOrdinal("lateFee"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("lateFee")));
                    decimal _serviceFee = (rs.IsDBNull(rs.GetOrdinal("serviceFee"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("serviceFee")));
                    decimal _lastPayment = (rs.IsDBNull(rs.GetOrdinal("lastPayment"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("lastPayment")));
                    decimal _remainingBalance = (rs.IsDBNull(rs.GetOrdinal("remaining_balance"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("remaining_balance")));
                    string _term = (rs.IsDBNull(rs.GetOrdinal("term"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("term"))).Trim();
                    string _payFreq = (rs.IsDBNull(rs.GetOrdinal("payFreq"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("payFreq"))).Trim();
                    string _membershipStatus = (rs.IsDBNull(rs.GetOrdinal("membershipStatus"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("membershipStatus"))).Trim();
                    decimal _periodRate = (rs.IsDBNull(rs.GetOrdinal("periodRate"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("periodRate")));
                    DateTime _lastPaymentDate = (rs.IsDBNull(rs.GetOrdinal("lastPaidDate"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("lastPaidDate")));
                    DateTime _delinquentDate = (rs.IsDBNull(rs.GetOrdinal("delinquentDate"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("delinquentDate")));
                    string _membershipSubType = (rs.IsDBNull(rs.GetOrdinal("membershipSubType"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("membershipSubType"))).Trim();

                    string _userEmail = (rs.IsDBNull(rs.GetOrdinal("userEmail"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userEmail"))).Trim();
                    string _userFirstName = (rs.IsDBNull(rs.GetOrdinal("userFirstName"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userFirstName"))).Trim();
                    string _userLastName = (rs.IsDBNull(rs.GetOrdinal("userLastName"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userLastName"))).Trim();
                    string _userAddress1 = (rs.IsDBNull(rs.GetOrdinal("userAddress1"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userAddress1"))).Trim();
                    string _userAddress2 = (rs.IsDBNull(rs.GetOrdinal("userAddress2"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userAddress2"))).Trim();
                    string _userCity = (rs.IsDBNull(rs.GetOrdinal("userCity"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userCity"))).Trim();
                    string _userState = (rs.IsDBNull(rs.GetOrdinal("userState"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userState"))).Trim();
                    string _userZip = (rs.IsDBNull(rs.GetOrdinal("userZip"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userZip"))).Trim();
                    string _displayName = (rs.IsDBNull(rs.GetOrdinal("displayName"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("displayName"))).Trim();
                    string _homePhone = (rs.IsDBNull(rs.GetOrdinal("homePhone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("homePhone"))).Trim();
                    string _workPhone = (rs.IsDBNull(rs.GetOrdinal("workPhone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("workPhone"))).Trim();
                    string _cellPhone = (rs.IsDBNull(rs.GetOrdinal("cellPhone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("cellPhone"))).Trim();
                    DateTime _isEftPendingDate = (rs.IsDBNull(rs.GetOrdinal("isEftPending"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("isEftPending")));
                    string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();
                    string _invPrefString = (rs.IsDBNull(rs.GetOrdinal("invoice_pref"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("invoice_pref"))).Trim();
                    bool _doNotContact = (rs.IsDBNull(rs.GetOrdinal("do_not_contact"))) ? false : (rs.GetBoolean(rs.GetOrdinal("do_not_contact")));
                    string _eftPref = (rs.IsDBNull(rs.GetOrdinal("eftPref"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("eftPref"))).Trim();

                    string _emergencyContactName = (rs.IsDBNull(rs.GetOrdinal("em_contact_name"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("em_contact_name"))).Trim();
                    string _emergencyContactPhone = (rs.IsDBNull(rs.GetOrdinal("em_contact_phone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("em_contact_phone"))).Trim();

                    int _addOnCountAdvanced = (rs.IsDBNull(rs.GetOrdinal("addOnCountAdvanced"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("addOnCountAdvanced")));
                    int _addOnCountSimple = (rs.IsDBNull(rs.GetOrdinal("addOnCountSimple"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("addOnCountSimple")));

                    if (_uid != null && !string.Empty.Equals(_uid.Trim()))
                    {
                        MembershipOpenBalancesVO vo = new MembershipOpenBalancesVO(_uid);
                        vo.AccessCode = _accessCode;
                        vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum(_userStatus);
                        vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum(_userGroup);
                        vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);
                        vo.DisplayName = string.Format("{0}-{1}", _userLastName, _userFirstName);
                        vo.ExpectedPayments = _amountExpected;
                        vo.Payments = _amountPaid;
                        vo.AmountDue = _amountDue;
                        vo.LateFee = _lateFee;
                        vo.ServiceFee = _serviceFee;
                        vo.LastPayment = _lastPayment;
                        vo.RemainingBalance = _remainingBalance;
                        vo.Term = _term;
                        try
                        {
                            vo.PayFreq = (MembershipPayFrequencyEnum)payFrequencyDbMapper.mapStringAsEnum(string.Format("{0}", _payFreq));
                        }
                        catch (Exception ex1)
                        {
                            log.Error(string.Format("_payFreq [{0}] incorrect for [{1}].", _payFreq, _uid), ex1);
                        }
                        try
                        {
                            vo.MembershipStatus = (MembershipStatusEnum)membershipStatusDbMapper.mapStringAsEnum(string.Format("{0}", _membershipStatus));
                        }
                        catch (Exception ex1)
                        {
                            log.Error(string.Format("_membershipStatus [{0}] incorrect for [{1}].", _membershipStatus, _uid), ex1);
                        }
                        
                        vo.LastPaymentDate = _lastPaymentDate;
                        vo.MonthlyRate = _periodRate;
                        vo.Notes = _notes;
                        vo.EmergencyContactName = _emergencyContactName;
                        vo.EmergencyContactPhone = _emergencyContactPhone;
                        vo.UserEmail = _userEmail;
                        vo.MailingLabelData = new MailingLabelData(_userLastName, _userFirstName, _userAddress1, _userAddress2, _userCity, _userState, _userZip);
                        vo.MailingLabelData.HomePhone = _homePhone;
                        vo.MailingLabelData.WorkPhone = _workPhone;
                        vo.MailingLabelData.CellPhone = _cellPhone;
                        vo.MailingLabelData.Email = _userEmail;
                        vo.MailingLabelData.DoNotContact = _doNotContact;
                        vo.DelinquentDate = _delinquentDate;
                        vo.IsEftPendingDate = _isEftPendingDate;
                        vo.DaysDelinquent = 0;
                        if (DateTime.MinValue.CompareTo(_delinquentDate) < 0)
                        {
                            TimeSpan span = rightNow - _delinquentDate;
                            int minutesLeft = (int)(span.Ticks / TimeSpan.TicksPerMinute);
                            double daysDelinq = (double)minutesLeft / (double)1440;

                            vo.DaysDelinquent = Convert.ToInt32(daysDelinq) - 1;
                        }

                        vo.DelinquencyLevel = DelinquencyLevelEnum.NONE;

                        if (vo.DaysDelinquent >= 1 && vo.DaysDelinquent <= 30)
                        {
                            vo.DelinquencyLevel = DelinquencyLevelEnum.LOW;
                        }

                        if (vo.DaysDelinquent >= 31 && vo.DaysDelinquent <= 60)
                        {
                            vo.DelinquencyLevel = DelinquencyLevelEnum.MEDIUM;
                        }

                        if (vo.DaysDelinquent >= 61 && vo.DaysDelinquent <= 90)
                        {
                            vo.DelinquencyLevel = DelinquencyLevelEnum.HIGH;
                        }

                        if (vo.DaysDelinquent > 90)
                        {
                            vo.DelinquencyLevel = DelinquencyLevelEnum.URGENT;
                        }

                        if (_invPrefString != null && !string.Empty.Equals(_invPrefString.Trim()))
                        {
                            try
                            {
                                vo.InvoicePreference = (InvoicePreferenceEnum)invoicePreferenceEnumDbMapper.mapStringAsEnum(_invPrefString);
                            }
                            catch (Exception ex12)
                            {
                                log.Error(string.Format("Incorrect Invoice Preference [{0}] in billing_master", _invPrefString), ex12);
                            }
                        }

                        vo.EftPref = (string.Empty.Equals(_eftPref.Trim())) ? 0x0000 : (EFTPreference)eftPrefDbMapper.mapStringAsEnum(string.Format("{0}", _eftPref));


                        vo.MembershipSubType = 0x0000;
                        try
                        {
                            vo.MembershipSubType = (MembershipSubtypeEnum)membershipSubtypeDbMapper.mapStringAsEnum(string.Format("{0}", _membershipSubType));
                        }
                        catch (Exception ex1)
                        {
                            log.Error(string.Format("Unable to convert _membershipSubType [{0}] to membershipSubType.", _membershipSubType), ex1);
                        }

                        vo.EffectiveDate = searchCriteria.EffectiveDate;

                        vo.AddOnCountAdvanced = _addOnCountAdvanced;
                        vo.AddOnCountSimple = _addOnCountSimple;
                        log.Debug(vo);

                        if (searchCriteria.DelinquencyLevel == 0x0000)
                        {
                            list.Add(vo);
                        }
                        else
                        {
                            if (searchCriteria.DelinquencyLevel.Equals(vo.DelinquencyLevel))
                            {
                                list.Add(vo);
                            }
                        }
                    }
                }

                // Close reader.
                rs.Close();

                connection.Close();

            }
            catch (Exception ex)
            {
                string sql_proc_str = string.Empty;
                if (command != null)
                {
                    sql_proc_str = string.Format("[{0}]", command.CommandText);
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {
                        sql_proc_str = string.Format("{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals(command.Parameters[i].Value) ? "NULL" : string.Format("'{0}'", command.Parameters[i].Value));
                    }
                }

                log.Error(string.Format("Exception thrown running query. [{0}]", sql_proc_str), ex);
                throw ex;
            }
            finally
            {

                if (connection != null)
                {
                    connection.Close();
                    connection = null;
                }
            }

            // contact history results
            Hashtable contactHistoryCache = new Hashtable();

            ContactEventDAO cedao = new ContactEventDAO();

            foreach (MembershipOpenBalancesVO vo in list)
            {
                // already cached?
                if (contactHistoryCache.ContainsKey(vo.AccessCode))
                {
                    vo.ContactHistoryResults = (ArrayList)contactHistoryCache[vo.AccessCode];
                    continue;
                }

                // load results
                ContactEventListSearchVO celsvo = new ContactEventListSearchVO();
                celsvo.Uid = vo.UserId;
                celsvo.UserGroup = vo.UserGroup;

                ArrayList initialResults = cedao.retrieveContactEventList(celsvo);
                ArrayList contactHistResults = new ArrayList();

                foreach (ContactEventVO cvo in initialResults)
                {
                    if (cvo.DisplayOnCheckin)
                    {
                        contactHistResults.Add(cvo);
                    }
                }

                vo.ContactHistoryResults = contactHistResults;

                contactHistoryCache.Add(vo.AccessCode, contactHistResults);
            }

            return list;
        }				

		
		public ArrayList retrieveMembershipOpenBalancesList( OpenBalanceMembershipListSearchVO searchCriteria  )
		{			

			if( searchCriteria == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "OpenBalanceMembershipListSearchVO [{0}] is null.", searchCriteria );
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("OpenBalanceMembershipListSearchVO is [{0}]", searchCriteria) );
			}			
			
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;
			SqlParameter idParam = null;
			ArrayList list = new ArrayList();
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			MembershipPayFrequencyEnumDatabaseMapper payFrequencyDbMapper = MembershipPayFrequencyEnumDatabaseMapper.getInstance();			
			MembershipStatusEnumDatabaseMapper membershipStatusDbMapper = MembershipStatusEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();
			UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
			UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
			InvoicePreferenceEnumDatabaseMapper invoicePreferenceEnumDbMapper = InvoicePreferenceEnumDatabaseMapper.getInstance();
			MembershipSubtypeEnumDatabaseMapper membershipSubtypeDbMapper = MembershipSubtypeEnumDatabaseMapper.getInstance();
			EFTPreferenceEnumDatabaseMapper eftPrefDbMapper = EFTPreferenceEnumDatabaseMapper.getInstance();
			
			DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();

            string sqltimeoutstr = ShapeConfiguration.AppSettings("SQLTimeOut");
            int sqltimeout = 240;
            if (!Int32.TryParse(sqltimeoutstr, out sqltimeout))
            {
                sqltimeout = 240;
            } 
            try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();			
				
				// 2. Query to retrieve results.
				command = new SqlCommand( "sp_get_membership_open_balance", connection );
                command.CommandTimeout = sqltimeout;
				command.CommandType = CommandType.StoredProcedure;								
				
				idParam = command.Parameters.Add("@effectiveDate", SqlDbType.SmallDateTime );
				idParam.Value = string.Format( "{0:MM/dd/yyyy}", searchCriteria.EffectiveDate );
				
				idParam = command.Parameters.Add("@billing_type", SqlDbType.NVarChar, 1 );
				idParam.Value = "M";	
				
				idParam = command.Parameters.Add("@access_code", SqlDbType.Int );
				idParam.Value = searchCriteria.AccessCode;
				if( searchCriteria.AccessCode <= 0 )
				{
					idParam.Value = DBNull.Value;
				}				

				idParam = command.Parameters.Add("@pay_group", SqlDbType.NVarChar, 50 );
				idParam.Value = DBNull.Value;
				
				idParam = command.Parameters.Add("@membershipStatus", SqlDbType.NVarChar, 1 );
				if( searchCriteria.MembershipStatus == 0x0000 )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = membershipStatusDbMapper.mapEnumAsString( searchCriteria.MembershipStatus );
				}									
				
				idParam = command.Parameters.Add("@amountDueAmt", SqlDbType.Real );
				idParam.Value = DBNull.Value;
				if( searchCriteria.AmountDueAmt.CompareTo( (decimal)0.00 ) > 0 )
				{
					idParam.Value = searchCriteria.AmountDueAmt;
				}				
				
				idParam = command.Parameters.Add("@daysPastDue", SqlDbType.Int );
				idParam.Value = DBNull.Value;
				if( searchCriteria.DaysPastDue > -1 )
				{
					idParam.Value = searchCriteria.DaysPastDue;
				}					
				
				idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.Cid != null && !string.Empty.Equals( searchCriteria.Cid.Trim() ) )
				{
					idParam.Value = searchCriteria.Cid;				
				}
				
				idParam = command.Parameters.Add("@uid_operator", SqlDbType.NVarChar, 2 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.CidOperator != null && !string.Empty.Equals( searchCriteria.CidOperator.Trim() ) )
				{
					idParam.Value = searchCriteria.CidOperator;	
				}		
												
				idParam = command.Parameters.Add("@user_status", SqlDbType.NVarChar, 1 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.UserStatus != 0x0000 )
				{
					idParam.Value = userStatusDbMapper.mapEnumAsString( searchCriteria.UserStatus );
				}		
				
				idParam = command.Parameters.Add("@member_types", SqlDbType.NVarChar, 255 );
				idParam.Value = DBNull.Value;
				if( !string.Empty.Equals( searchCriteria.MemberTypesDelimitedString ) )
				{
					idParam.Value = searchCriteria.MemberTypesDelimitedString;
				}
				
				idParam = command.Parameters.Add("@membership_types", SqlDbType.NVarChar, -1 );
				idParam.Value = DBNull.Value;
				if( !string.Empty.Equals( searchCriteria.MembershipTypesDelimitedString ) )
				{
					idParam.Value = searchCriteria.MembershipTypesDelimitedString;
				}
												
				idParam = command.Parameters.Add("@user_group", SqlDbType.NVarChar, 1 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.UserGroup != 0x0000 )
				{
					idParam.Value = userTypeDbMapper.mapEnumAsString( searchCriteria.UserGroup );
				}		
				
				idParam = command.Parameters.Add("@user_status_active_and_inactive", SqlDbType.Bit );
				idParam.Value = searchCriteria.UserStatusActiveAndInactive;
				
				idParam = command.Parameters.Add("@countAddons", SqlDbType.Bit );
				idParam.Value = searchCriteria.CountAddOns;
    			
				if (log.IsDebugEnabled)
				{
					string sql_proc_str = string.Format( "[{0}]", command.CommandText );
					for (int i = 0; i < command.Parameters.Count; i++)
					{
						sql_proc_str = string.Format( "{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals( command.Parameters[i].Value ) ? "NULL" : string.Format( "'{0}'", command.Parameters[i].Value ) );
					}
				
					log.Debug( sql_proc_str );
				}
                
				// Retrieve data
				rs = command.ExecuteReader();							

				while( rs.Read() )
				{	
					//billing_type varchar(1), access_code int, pay_group varchar(50), uid varchar(40), notes varchar(512), amountExpected decimal, 
					//amountPaid decimal, amountDue decimal, term char(10) null, payFreq char(1) null, periodRate smallmoney null, lastPaidDate smalldatetime null )'
                    string _notesContact = (rs.IsDBNull(rs.GetOrdinal("notesContact"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("notesContact"))).Trim();
                    DateTime _lastContact = (rs.IsDBNull(rs.GetOrdinal("lastContact"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("lastContact")));
                    string _contactResult = (rs.IsDBNull(rs.GetOrdinal("contactResult"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("contactResult"))).Trim();
                    string _contactRep = (rs.IsDBNull(rs.GetOrdinal("contactRep"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("contactRep"))).Trim();
                    string _smsGateway = (rs.IsDBNull(rs.GetOrdinal("smsGateway"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("smsGateway"))).Trim();
                    bool _smsActiveGateway = (rs.IsDBNull(rs.GetOrdinal("smsGatewayActive"))) ? false : (rs.GetBoolean(rs.GetOrdinal("smsGatewayActive")));
                    int _smsGatewayId = (rs.IsDBNull(rs.GetOrdinal("smsGatewayId"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("smsGatewayId")));
                    int _accessCode = ( rs.IsDBNull( rs.GetOrdinal("access_code") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("access_code") ) );
					string _userStatus = ( rs.IsDBNull( rs.GetOrdinal("user_status") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("user_status") ) ).Trim();					
					string _userGroup = ( rs.IsDBNull( rs.GetOrdinal("userGroup") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userGroup") ) ).Trim();
					string _billing_type = ( rs.IsDBNull( rs.GetOrdinal("billing_type") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("billing_type") ) ).Trim();
					string _pay_group = ( rs.IsDBNull( rs.GetOrdinal("pay_group") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("pay_group") ) ).Trim();
					string _uid = ( rs.IsDBNull( rs.GetOrdinal("uid") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("uid") ) ).Trim();
					string _notes = ( rs.IsDBNull( rs.GetOrdinal("notes") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("notes") ) ).Trim();
					decimal _amountExpected = ( rs.IsDBNull( rs.GetOrdinal("amountExpected") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountExpected") ) );
					decimal _amountPaid = ( rs.IsDBNull( rs.GetOrdinal("amountPaid") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountPaid") ) );
					decimal _amountDue = ( rs.IsDBNull( rs.GetOrdinal("amountDue") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountDue") ) );
                    decimal _lateFee = (rs.IsDBNull(rs.GetOrdinal("lateFee"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("lateFee")));
                    decimal _serviceFee = (rs.IsDBNull(rs.GetOrdinal("serviceFee"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("serviceFee")));
                    decimal _lastPayment = (rs.IsDBNull(rs.GetOrdinal("lastPayment"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("lastPayment")));
                    decimal _remainingBalance = ( rs.IsDBNull( rs.GetOrdinal("remaining_balance") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("remaining_balance") ) );
					string _term = ( rs.IsDBNull( rs.GetOrdinal("term") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("term") ) ).Trim();
					string _payFreq = ( rs.IsDBNull( rs.GetOrdinal("payFreq") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("payFreq") ) ).Trim();
					string _membershipStatus = ( rs.IsDBNull( rs.GetOrdinal("membershipStatus") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("membershipStatus") ) ).Trim();
					decimal _periodRate = ( rs.IsDBNull( rs.GetOrdinal("periodRate") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("periodRate") ) );					
					DateTime _lastPaymentDate = ( rs.IsDBNull( rs.GetOrdinal("lastPaidDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("lastPaidDate") ) );								    					
					DateTime _delinquentDate = ( rs.IsDBNull( rs.GetOrdinal("delinquentDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("delinquentDate") ) );								    					
					string _membershipSubType = ( rs.IsDBNull( rs.GetOrdinal("membershipSubType") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("membershipSubType") ) ).Trim();
					
					string _userEmail = ( rs.IsDBNull( rs.GetOrdinal("userEmail") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userEmail") ) ).Trim();
					string _userFirstName = ( rs.IsDBNull( rs.GetOrdinal("userFirstName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userFirstName") ) ).Trim();
					string _userLastName = ( rs.IsDBNull( rs.GetOrdinal("userLastName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userLastName") ) ).Trim();
					string _userAddress1 = ( rs.IsDBNull( rs.GetOrdinal("userAddress1") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userAddress1") ) ).Trim();
					string _userAddress2 = ( rs.IsDBNull( rs.GetOrdinal("userAddress2") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userAddress2") ) ).Trim();
					string _userCity = ( rs.IsDBNull( rs.GetOrdinal("userCity") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userCity") ) ).Trim();
					string _userState = ( rs.IsDBNull( rs.GetOrdinal("userState") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userState") ) ).Trim();
					string _userZip = ( rs.IsDBNull( rs.GetOrdinal("userZip") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userZip") ) ).Trim();					
					string _displayName = ( rs.IsDBNull( rs.GetOrdinal("displayName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("displayName") ) ).Trim();															
					string _homePhone = ( rs.IsDBNull( rs.GetOrdinal("homePhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("homePhone") ) ).Trim();										
					string _workPhone = ( rs.IsDBNull( rs.GetOrdinal("workPhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("workPhone") ) ).Trim();										
					string _cellPhone = ( rs.IsDBNull( rs.GetOrdinal("cellPhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("cellPhone") ) ).Trim();															
					DateTime _isEftPendingDate = ( rs.IsDBNull( rs.GetOrdinal("isEftPending") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("isEftPending") ) );
                    string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();
					string _invPrefString = ( rs.IsDBNull( rs.GetOrdinal("invoice_pref") ) )  ? string.Empty : ( rs.GetString( rs.GetOrdinal("invoice_pref") ) ).Trim();
					bool _doNotContact = ( rs.IsDBNull( rs.GetOrdinal("do_not_contact") ) ) ? false : ( rs.GetBoolean( rs.GetOrdinal("do_not_contact") ) );
					string _eftPref = ( rs.IsDBNull( rs.GetOrdinal("eftPref") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("eftPref") ) ).Trim();
					
					string _emergencyContactName = ( rs.IsDBNull( rs.GetOrdinal("em_contact_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("em_contact_name") ) ).Trim();
					string _emergencyContactPhone = ( rs.IsDBNull( rs.GetOrdinal("em_contact_phone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("em_contact_phone") ) ).Trim();
					
					int _addOnCountAdvanced = ( rs.IsDBNull( rs.GetOrdinal("addOnCountAdvanced") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("addOnCountAdvanced") ) );
					int _addOnCountSimple = ( rs.IsDBNull( rs.GetOrdinal("addOnCountSimple") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("addOnCountSimple") ) );
                    bool _ignoreOpenBalance = (rs.IsDBNull(rs.GetOrdinal("ignoreOpenBalance"))) ? false : (rs.GetBoolean(rs.GetOrdinal("ignoreOpenBalance")));
                    int _autoLockPastDueDays = (rs.IsDBNull(rs.GetOrdinal("autoLockPastDueDays"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("autoLockPastDueDays")));
                    
                    if ( _uid != null && !string.Empty.Equals( _uid.Trim() ) )
					{		
						MembershipOpenBalancesVO vo = new MembershipOpenBalancesVO( _uid );
						vo.AccessCode = _accessCode;
						vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum( _userStatus );
						vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum( _userGroup );
                        vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);
						vo.DisplayName = string.Format( "{0}-{1}", _userLastName, _userFirstName );
						vo.ExpectedPayments = _amountExpected;
						vo.Payments = _amountPaid;
						vo.AmountDue = _amountDue;
                        vo.LateFee = _lateFee;
                        vo.ServiceFee = _serviceFee;
                        vo.LastPayment = _lastPayment;
                        vo.RemainingBalance = _remainingBalance;
						vo.Term = _term;
						try
						{
							vo.PayFreq = ( MembershipPayFrequencyEnum )payFrequencyDbMapper.mapStringAsEnum( string.Format( "{0}", _payFreq ) );
						}
						catch( Exception ex1 )
						{
							log.Error( string.Format( "_payFreq [{0}] incorrect for [{1}].", _payFreq, _uid ), ex1 );
						}
						try
						{
							vo.MembershipStatus = ( MembershipStatusEnum )membershipStatusDbMapper.mapStringAsEnum( string.Format( "{0}", _membershipStatus ) );
						}
						catch( Exception ex1 )
						{
							log.Error( string.Format( "_membershipStatus [{0}] incorrect for [{1}].", _membershipStatus, _uid ), ex1 );
						}						
												
						vo.LastPaymentDate = _lastPaymentDate;
						vo.MonthlyRate = _periodRate;
						vo.Notes = _notes;
						vo.EmergencyContactName = _emergencyContactName;
						vo.EmergencyContactPhone = _emergencyContactPhone;
						vo.UserEmail = _userEmail;
						vo.MailingLabelData = new MailingLabelData( _userLastName, _userFirstName, _userAddress1, _userAddress2, _userCity, _userState, _userZip );	
						vo.MailingLabelData.HomePhone = _homePhone;
						vo.MailingLabelData.WorkPhone = _workPhone;
						vo.MailingLabelData.CellPhone = _cellPhone;
						vo.MailingLabelData.Email = _userEmail;
						vo.MailingLabelData.DoNotContact = _doNotContact;
                        vo.SmsGateway = _smsGateway;
                        vo.SmsGatewayActive = _smsActiveGateway;
                        vo.SmsGatewayId = _smsGatewayId;



						vo.DelinquentDate = _delinquentDate;
						vo.IsEftPendingDate =  _isEftPendingDate;
						vo.DaysDelinquent = 0;
						if( DateTime.MinValue.CompareTo( _delinquentDate ) < 0 )
						{							
							TimeSpan span = rightNow - _delinquentDate;
							int minutesLeft = (int)(span.Ticks / TimeSpan.TicksPerMinute);
							double daysDelinq =  (double)minutesLeft / (double)1440;		
							
							vo.DaysDelinquent = Convert.ToInt32(daysDelinq) - 1;
						}

						vo.DelinquencyLevel = DelinquencyLevelEnum.NONE;
						
						if( vo.DaysDelinquent >= 1 && vo.DaysDelinquent <= 30 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.LOW;
						}						
						
						if( vo.DaysDelinquent >= 31 && vo.DaysDelinquent <= 60 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.MEDIUM;
						}
						
						if( vo.DaysDelinquent >= 61 && vo.DaysDelinquent <= 90 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.HIGH;
						}
						
						if( vo.DaysDelinquent > 90 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.URGENT;
						}						
						
						if( _invPrefString != null && !string.Empty.Equals( _invPrefString.Trim() ) )
						{
							try
							{
								vo.InvoicePreference = (InvoicePreferenceEnum)invoicePreferenceEnumDbMapper.mapStringAsEnum( _invPrefString );
							}
							catch( Exception ex12 )
							{
								log.Error( string.Format( "Incorrect Invoice Preference [{0}] in billing_master", _invPrefString ), ex12 );
							}
						}	
						
						vo.EftPref = ( string.Empty.Equals( _eftPref.Trim() ) )? 0x0000 : ( EFTPreference )eftPrefDbMapper.mapStringAsEnum( string.Format( "{0}", _eftPref ) );
						

						vo.MembershipSubType = 0x0000;
						try
						{
							vo.MembershipSubType = ( MembershipSubtypeEnum )membershipSubtypeDbMapper.mapStringAsEnum( string.Format( "{0}", _membershipSubType ) );
						}
						catch( Exception ex1 )
						{
							log.Error( string.Format( "Unable to convert _membershipSubType [{0}] to membershipSubType.", _membershipSubType ), ex1 );
						}						
						
						vo.EffectiveDate = searchCriteria.EffectiveDate;
						
						vo.AddOnCountAdvanced = _addOnCountAdvanced;
						vo.AddOnCountSimple = _addOnCountSimple;
					    vo.NotesContact = _notesContact;
                        if (_lastContact != DateTime.MinValue)
                        {
                            if (String.IsNullOrEmpty(_contactRep))
                                vo.NotesContact = string.Format("{0:g}: {1}", _lastContact, _notesContact);
                            else
                                vo.NotesContact = string.Format("{0:g} by {2}: {1}", _lastContact, _notesContact, _contactRep);
                        }
                        vo.ContactResult = _contactResult;
                        
                        vo.SmsGateway = _smsGateway;
                        vo.SmsGatewayActive = _smsActiveGateway;
                        vo.SmsGatewayId = _smsGatewayId;
						vo.IgnoreOpenBalance = _ignoreOpenBalance;
						vo.AutoLockPastDueDays = _autoLockPastDueDays;

                        log.Debug( vo );
						
						if( searchCriteria.DelinquencyLevel == 0x0000 )
						{
							list.Add( vo );													
						}
						else
						{
							if( searchCriteria.DelinquencyLevel.Equals( vo.DelinquencyLevel ) )
							{
								list.Add( vo );
							}
						}
					}										
				}
								
				// Close reader.
				rs.Close();								
				
				connection.Close();

			}
			catch( Exception ex )
			{
				string sql_proc_str = string.Empty;
				if( command != null )
				{
					sql_proc_str = string.Format( "[{0}]", command.CommandText );
					for( int i = 0; i < command.Parameters.Count; i++ )
					{
						sql_proc_str = string.Format( "{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals( command.Parameters[i].Value ) ? "NULL" : string.Format( "'{0}'", command.Parameters[i].Value ) );
					}
				}
				
				log.Error( string.Format( "Exception thrown running query. [{0}]", sql_proc_str ), ex );
				throw ex;
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
			
			// contact history results
			Hashtable contactHistoryCache = new Hashtable();
			
			ContactEventDAO cedao = new ContactEventDAO();
				
			foreach( MembershipOpenBalancesVO vo in list )
			{
				// already cached?
				if( contactHistoryCache.ContainsKey( vo.AccessCode ) )
				{
					vo.ContactHistoryResults = (ArrayList)contactHistoryCache[ vo.AccessCode ];
					continue;
				}
				
				// load results
				ContactEventListSearchVO celsvo = new ContactEventListSearchVO();
				celsvo.Uid = vo.UserId;
				celsvo.UserGroup = vo.UserGroup;
	
				ArrayList initialResults = cedao.retrieveContactEventList( celsvo );
				ArrayList contactHistResults = new ArrayList();
				
				foreach( ContactEventVO cvo in initialResults )
				{
					//if( cvo.DisplayOnCheckin ) // for open balance, we display all
					{
						contactHistResults.Add( cvo );
					}
				}
				
				vo.ContactHistoryResults = contactHistResults;
				
				contactHistoryCache.Add( vo.AccessCode, contactHistResults );
			}		
			
			return list;
		}				
		
		public ArrayList retrievePosOpenBalanceList( OpenBalancePosListSearchVO searchCriteria  )
		{			
			
			if( searchCriteria == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "OpenBalancePosListSearchVO [{0}] is null.", searchCriteria );
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("OpenBalancePosListSearchVO is [{0}]", searchCriteria) );
			}
			
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;
			SqlParameter idParam = null;
			ArrayList list = new ArrayList();
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
			UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
			ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();
			InvoicePreferenceEnumDatabaseMapper invoicePreferenceEnumDbMapper = InvoicePreferenceEnumDatabaseMapper.getInstance();

            DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();
            
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();

				// 2. Query to retrieve results.
				command = new SqlCommand( "[sp_get_pos_open_balance]", connection );
				command.CommandType = CommandType.StoredProcedure;				
				
				idParam = command.Parameters.Add("@effective_date", SqlDbType.SmallDateTime );
				idParam.Value = string.Format( "{0:MM/dd/yyyy}", searchCriteria.EffectiveDate );
				if( searchCriteria.EffectiveDate.CompareTo( DateTime.MinValue ) <= 0 )
				{
					idParam.Value = string.Format( "{0:MM/dd/yyyy}", rightNow );
				}								
				
				idParam = command.Parameters.Add("@billing_type", SqlDbType.NVarChar, 1 );
				idParam.Value = "C";	
				
				idParam = command.Parameters.Add("@access_code", SqlDbType.Int );
				idParam.Value = DBNull.Value;
				if( searchCriteria.AccessCode > 0 )
				{
					idParam.Value = searchCriteria.AccessCode;				
				}				
				
				idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.Uid != null && !string.Empty.Equals( searchCriteria.Uid.Trim() ) )
				{
					idParam.Value = searchCriteria.Uid;				
				}
				
				idParam = command.Parameters.Add("@uid_operator", SqlDbType.NVarChar, 2 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.UidOperator != null && !string.Empty.Equals( searchCriteria.UidOperator.Trim() ) )
				{
					idParam.Value = searchCriteria.UidOperator;	
				}		
												
				idParam = command.Parameters.Add("@user_status", SqlDbType.NVarChar, 1 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.UserStatus != 0x0000 )
				{
					idParam.Value = userStatusDbMapper.mapEnumAsString( searchCriteria.UserStatus );
				}		
				
				idParam = command.Parameters.Add("@user_group", SqlDbType.NVarChar, 1 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.UserGroup != 0x0000 )
				{
					idParam.Value = userTypeDbMapper.mapEnumAsString( searchCriteria.UserGroup );
				}			
				
				idParam = command.Parameters.Add("@hide_negative", SqlDbType.Bit );
				idParam.Value = DBNull.Value;
				if( searchCriteria.HideNegative != null && !string.Empty.Equals( searchCriteria.HideNegative.Trim() ) )
				{
					idParam.Value = "Y".Equals( searchCriteria.HideNegative.Trim() ) ? true : false;
				}
				
				idParam = command.Parameters.Add("@pay_group", SqlDbType.NVarChar, 50 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.TransactionId > 0 )
				{
					idParam.Value = string.Format("{0}", searchCriteria.TransactionId);
				}

                idParam = command.Parameters.Add("@daysPastDue", SqlDbType.Int);
                idParam.Value = DBNull.Value;
                if (searchCriteria.DaysPastDue > -1)
                {
                    idParam.Value = searchCriteria.DaysPastDue;
                }

                idParam = command.Parameters.Add("@user_status_active_and_inactive", SqlDbType.Bit );
				idParam.Value = searchCriteria.UserStatusActiveAndInactive;

				if (log.IsDebugEnabled)
				{
					string sql_proc_str = string.Format( "[{0}]", command.CommandText );
					for (int i = 0; i < command.Parameters.Count; i++)
					{
						sql_proc_str = string.Format( "{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals( command.Parameters[i].Value ) ? "NULL" : string.Format( "'{0}'", command.Parameters[i].Value ) );
					}
				
					log.Debug( sql_proc_str );
				}
				
				// Retrieve data
				rs = command.ExecuteReader();							

				while( rs.Read() )
				{
                    string _notesContact = (rs.IsDBNull(rs.GetOrdinal("notesContact"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("notesContact"))).Trim();
                    DateTime _lastContact = (rs.IsDBNull(rs.GetOrdinal("lastContact"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("lastContact")));
                    string _contactResult = (rs.IsDBNull(rs.GetOrdinal("contactResult"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("contactResult"))).Trim();
                    string _contactRep = (rs.IsDBNull(rs.GetOrdinal("contactRep"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("contactRep"))).Trim();
                    int _accessCode = (rs.IsDBNull(rs.GetOrdinal("access_code"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("access_code")));
					string _notes = ( rs.IsDBNull( rs.GetOrdinal("notes") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("notes") ) ).Trim();					
					string _uid = ( rs.IsDBNull( rs.GetOrdinal("uid") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("uid") ) ).Trim();
					string _userGroup = ( rs.IsDBNull( rs.GetOrdinal("user_group") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("user_group") ) ).Trim();
					string _userStatus = ( rs.IsDBNull( rs.GetOrdinal("user_status") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("user_status") ) ).Trim();					
					decimal _amountDue = ( rs.IsDBNull( rs.GetOrdinal("amount_due") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amount_due") ) );								
					string _email = ( rs.IsDBNull( rs.GetOrdinal("email") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("email") ) ).Trim();					
					string _firstName = ( rs.IsDBNull( rs.GetOrdinal("first_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("first_name") ) ).Trim();					
					string _lastName = ( rs.IsDBNull( rs.GetOrdinal("last_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("last_name") ) ).Trim();
                    string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();

					string _billing_type = ( rs.IsDBNull( rs.GetOrdinal("billing_type") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("billing_type") ) ).Trim();
					string _pay_group = ( rs.IsDBNull( rs.GetOrdinal("pay_group") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("pay_group") ) ).Trim();
					decimal _amountExpected = ( rs.IsDBNull( rs.GetOrdinal("amountExpected") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountExpected") ) );
					decimal _amountPaid = ( rs.IsDBNull( rs.GetOrdinal("amountPaid") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountPaid") ) );
					DateTime _delinquentDate = ( rs.IsDBNull( rs.GetOrdinal("delinquentDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("delinquentDate") ) );
					DateTime _purchaseDate = ( rs.IsDBNull( rs.GetOrdinal("purchase_date") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("purchase_date") ) );
					
					string _userAddress1 = ( rs.IsDBNull( rs.GetOrdinal("userAddress1") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userAddress1") ) ).Trim();
					string _userAddress2 = ( rs.IsDBNull( rs.GetOrdinal("userAddress2") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userAddress2") ) ).Trim();
					string _userCity = ( rs.IsDBNull( rs.GetOrdinal("userCity") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userCity") ) ).Trim();
					string _userState = ( rs.IsDBNull( rs.GetOrdinal("userState") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userState") ) ).Trim();
					string _userZip = ( rs.IsDBNull( rs.GetOrdinal("userZip") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userZip") ) ).Trim();					
					string _displayName = ( rs.IsDBNull( rs.GetOrdinal("displayName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("displayName") ) ).Trim();					
					string _homePhone = ( rs.IsDBNull( rs.GetOrdinal("homePhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("homePhone") ) ).Trim();										
					string _workPhone = ( rs.IsDBNull( rs.GetOrdinal("workPhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("workPhone") ) ).Trim();
                    string _cellPhone = (rs.IsDBNull(rs.GetOrdinal("cellPhone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("cellPhone"))).Trim();
                    string _smsGateway = (rs.IsDBNull(rs.GetOrdinal("smsGateway"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("smsGateway"))).Trim();
                    bool _smsActiveGateway = (rs.IsDBNull(rs.GetOrdinal("smsGatewayActive"))) ? false : (rs.GetBoolean(rs.GetOrdinal("smsGatewayActive")));
                    int _smsGatewayId = (rs.IsDBNull(rs.GetOrdinal("smsGatewayId"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("smsGatewayId")));
                    bool _doNotContact = ( rs.IsDBNull( rs.GetOrdinal("do_not_contact") ) ) ? false : ( rs.GetBoolean( rs.GetOrdinal("do_not_contact") ) );
					string _invPrefString = ( rs.IsDBNull( rs.GetOrdinal("invoice_pref") ) )  ? string.Empty : ( rs.GetString( rs.GetOrdinal("invoice_pref") ) ).Trim();
                    string _transaction_desc = (rs.IsDBNull(rs.GetOrdinal("transaction_desc"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("transaction_desc"))).Trim();

					//Create new OpenBalancePosVO object
					OpenBalancePosVO vo = new OpenBalancePosVO();
					vo.AccessCode = _accessCode;
                    vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);
					vo.Notes = _notes;
					vo.Uid = _uid;
					vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum( _userGroup );
					vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum( _userStatus );
					vo.AmountDue = _amountDue;
					vo.Email = _email;
					vo.FirstName = _firstName;
					vo.LastName = _lastName;
					vo.DisplayName = _displayName;
					vo.ExpectedPayments = _amountExpected;
					vo.Payments = _amountPaid;

					vo.Notes = _notes;
					vo.MailingLabelData = new MailingLabelData( _lastName, _firstName, _userAddress1, _userAddress2, _userCity, _userState, _userZip );
					vo.MailingLabelData.DoNotContact = _doNotContact;
					vo.MailingLabelData.HomePhone = _homePhone;
					vo.MailingLabelData.WorkPhone = _workPhone;
					vo.MailingLabelData.CellPhone = _cellPhone;
					vo.MailingLabelData.Email = _email;
					vo.PurchaseDate = _purchaseDate;
                    vo.NotesContact = _notesContact;
                    if (_lastContact != DateTime.MinValue)
                    {
                        if (String.IsNullOrEmpty(_contactRep))
                            vo.NotesContact = string.Format("{0:g}: {1}", _lastContact, _notesContact);
                        else
                            vo.NotesContact = string.Format("{0:g} by {2}: {1}", _lastContact, _notesContact, _contactRep);
                    }
                    vo.ContactResult = _contactResult;
                    vo.SmsGateway = _smsGateway;
                    vo.SmsGatewayActive = _smsActiveGateway;
                    vo.SmsGatewayId = _smsGatewayId;


					if( _invPrefString != null && !string.Empty.Equals( _invPrefString.Trim() ) )
					{
						try
						{
							vo.InvoicePreference = (InvoicePreferenceEnum)invoicePreferenceEnumDbMapper.mapStringAsEnum( _invPrefString );
						}
						catch( Exception ex12 )
						{
							log.Error( string.Format( "Incorrect Invoice Preference [{0}] in billing_master", _invPrefString ), ex12 );
						}
					}						
					
					try
					{
						vo.TransactionId = Convert.ToInt64( _pay_group );
					}
					catch( Exception ex )
					{
						log.Error( string.Format( "Error converting transaction id to long. [{0}]", _pay_group ), ex );
						vo.TransactionId = (long)0;
					}
					
					vo.DelinquentDate = _delinquentDate;
					
					if( ( DateTime.MinValue.CompareTo( _delinquentDate ) < 0 ) )
					{							
						TimeSpan span = DateTime.Today - _delinquentDate.Date;
						int minutesLeft = (int)(span.Ticks / TimeSpan.TicksPerMinute);
						double daysDelinq =  (double)minutesLeft / (double)1440;		
						
						vo.DaysDelinquent = Convert.ToInt32(daysDelinq) - 1;
					}

					if( vo.DaysDelinquent < 0 )
					{
						vo.DaysDelinquent = 0;
					}
					
					vo.DelinquencyLevel = DelinquencyLevelEnum.NONE;
					
					if( vo.DaysDelinquent >= 1 && vo.DaysDelinquent <= 30 )
					{
						vo.DelinquencyLevel = DelinquencyLevelEnum.LOW;
					}
					else if( vo.DaysDelinquent >= 31 && vo.DaysDelinquent <= 60 )
					{
						vo.DelinquencyLevel = DelinquencyLevelEnum.MEDIUM;
					}
					else if( vo.DaysDelinquent >= 61 && vo.DaysDelinquent <= 90 )
					{
						vo.DelinquencyLevel = DelinquencyLevelEnum.HIGH;
					}
					else if( vo.DaysDelinquent > 90 )
					{
						vo.DelinquencyLevel = DelinquencyLevelEnum.URGENT;
					}						
					
					vo.EffectiveDate = searchCriteria.EffectiveDate;
                    vo.TransactionDescription = _transaction_desc;
					
					log.Debug( vo );
					
					if( searchCriteria.DelinquencyLevel == 0x0000 || searchCriteria.DelinquencyLevel.Equals( vo.DelinquencyLevel ) )
					{
						list.Add( vo );
					}
				}
								
				// Close reader.
				rs.Close();						
				
				connection.Close();

			}
			catch( Exception ex )
			{
				string sql_proc_str = string.Empty;
				if( command != null )
				{
					sql_proc_str = string.Format( "[{0}]", command.CommandText );
					for( int i = 0; i < command.Parameters.Count; i++ )
					{
						sql_proc_str = string.Format( "{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals( command.Parameters[i].Value ) ? "NULL" : string.Format( "'{0}'", command.Parameters[i].Value ) );
					}
				}
				
				log.Error( string.Format( "Exception thrown running query. [{0}]", sql_proc_str ), ex );
				throw ex;
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
			
			return list;
		}


        public OpenBalancePosVO retrieveExternalPosOpenBalance(int accessCode, string fac)
        {
            if (accessCode <= 0)
            {
                // Should throw an exception and log this.
                string msg = string.Format("accessCode is not valid.");
                log.Error(msg);
                throw new Exception(msg);
            }

            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("accessCode is [{0}]", accessCode));
            }

            OpenBalancePosVO vo = null;

            OpenBalancePosListSearchVO searchCriteria = new OpenBalancePosListSearchVO();
            searchCriteria.AccessCode = accessCode;
            searchCriteria.HideNegative = "N";

            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("OpenBalancePosListSearchVO is [{0}]", searchCriteria));
            }

            SqlConnection connection = null;
            SqlCommand command = null;
            SqlDataReader rs = null;
            SqlParameter idParam = null;
            ArrayList list = new ArrayList();

            // 1. Get the connection string.
            string connectionParam = ShapeConnection.retrieveConnection(ConfigurationManager.AppSettings["SQLDBConnString"]);
            UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
            UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
            ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();

            FacilityDAO fDao = new FacilityDAO();
            FacilityVO fVo = fDao.retrieveFacilityVO(fac);
            connectionParam = fVo.ConnectionString;
            
            DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();
            
            try
            {
                // Create and open connection.
                connection = new SqlConnection(connectionParam);
                connection.Open();

                // 2. Query to retrieve results.
                command = new SqlCommand("sp_get_open_balances_pos", connection);
                command.CommandType = CommandType.StoredProcedure;

                idParam = command.Parameters.Add("@accessCode", SqlDbType.Int);
                idParam.Value = searchCriteria.AccessCode;
                if (searchCriteria.AccessCode == 0)
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40);
                idParam.Value = searchCriteria.Uid;
                if (searchCriteria.Uid == null || string.Empty.Equals(searchCriteria.Uid.Trim()))
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@uidOperator", SqlDbType.NVarChar, 2);
                idParam.Value = searchCriteria.UidOperator;
                if (searchCriteria.UidOperator == null || string.Empty.Equals(searchCriteria.UidOperator.Trim()))
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@userStatus", SqlDbType.NVarChar, 1);
                if (searchCriteria.UserStatus == 0x0000)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = userStatusDbMapper.mapEnumAsString(searchCriteria.UserStatus);
                }

                idParam = command.Parameters.Add("@userType", SqlDbType.NVarChar, 1);
                if (searchCriteria.UserGroup == 0x0000)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = userTypeDbMapper.mapEnumAsString(searchCriteria.UserGroup);
                }

                idParam = command.Parameters.Add("@hideNegative", SqlDbType.NVarChar, 1);
                idParam.Value = searchCriteria.HideNegative;
                if (searchCriteria.HideNegative == null || string.Empty.Equals(searchCriteria.HideNegative.Trim()))
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@effdate", SqlDbType.SmallDateTime);
                if (searchCriteria.EffectiveDate.CompareTo(DateTime.MinValue) <= 0)
                {
                    idParam.Value = string.Format("{0:MM/dd/yyyy}", rightNow);
                }else {
                    idParam.Value = string.Format("{0:MM/dd/yyyy}", searchCriteria.EffectiveDate);
                }

                if (log.IsDebugEnabled)
                {
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {
                        log.Debug(string.Format("{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value));
                    }
                }

                // Retrieve data
                rs = command.ExecuteReader();

                while (rs.Read())
                {
                    int _accessCode = (rs.IsDBNull(rs.GetOrdinal("access_code"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("access_code")));
                    string _notes = (rs.IsDBNull(rs.GetOrdinal("notes"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("notes"))).Trim();
                    string _uid = (rs.IsDBNull(rs.GetOrdinal("uid"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("uid"))).Trim();
                    string _userGroup = (rs.IsDBNull(rs.GetOrdinal("user_group"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("user_group"))).Trim();
                    string _userStatus = (rs.IsDBNull(rs.GetOrdinal("user_status"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("user_status"))).Trim();
                    decimal _amountDue = (rs.IsDBNull(rs.GetOrdinal("amount_due"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amount_due")));
                    string _email = (rs.IsDBNull(rs.GetOrdinal("email"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("email"))).Trim();
                    string _firstName = (rs.IsDBNull(rs.GetOrdinal("first_name"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("first_name"))).Trim();
                    string _lastName = (rs.IsDBNull(rs.GetOrdinal("last_name"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("last_name"))).Trim();
                    string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();
                    DateTime _isEftPendingDate = (rs.IsDBNull(rs.GetOrdinal("isEftPending"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("isEftPending")));

                    //Create new PaymentVO object
                    vo = new OpenBalancePosVO();
                    vo.AccessCode = _accessCode;
                    vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);
                    vo.Notes = _notes;
                    vo.Uid = _uid;
                    vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum(_userGroup);
                    vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum(_userStatus);
                    vo.AmountDue = _amountDue;
                    vo.Email = _email;
                    vo.FirstName = _firstName;
                    vo.LastName = _lastName;
                    vo.IsEftPendingDate = _isEftPendingDate;

                    // Add to ArrayList as a validation check
                    list.Add(vo);

                }

                // Close reader.
                rs.Close();

                connection.Close();

            }
            catch (Exception e)
            {
                log.Error(e.ToString());
                throw e;
            }
            finally
            {

                if (connection != null)
                {
                    connection.Close();
                    connection = null;
                }
            }

            // Check that only one OpenBalancePosVO object was created.
            if (list.Count > 1)
            {
                log.Error("More than one OpenBalancePosVO was retrieved.");

                // Temporarily clear list.
                list = new ArrayList();
            }

            if (list.Count == 1)
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug("One OpenBalancePosVO retrieved from query and returned.");
                }
                vo = (OpenBalancePosVO)list[0];
                if (log.IsDebugEnabled)
                {
                    log.Debug(string.Format("Retrieved OpenBalancePosVO [{0}].", vo));
                }
            }

            return vo;
        }

        public OpenBalancePosVO retrievePosOpenBalance(int accessCode, SqlConnection connection, SqlTransaction transaction)
        {
            if (accessCode <= 0)
            {
                // Should throw an exception and log this.
                string msg = string.Format("accessCode is not valid.");
                log.Error(msg);
                throw new Exception(msg);
            }

            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("accessCode is [{0}]", accessCode));
            }

            OpenBalancePosVO vo = null;

            OpenBalancePosListSearchVO searchCriteria = new OpenBalancePosListSearchVO();
            searchCriteria.AccessCode = accessCode;
            searchCriteria.HideNegative = "N";

            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("OpenBalancePosListSearchVO is [{0}]", searchCriteria));
            }

            SqlCommand command = null;
            SqlDataReader rs = null;
            SqlParameter idParam = null;
            ArrayList list = new ArrayList();

            // 1. Get the connection string.
            UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
            UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
            ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();

            DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();

            try
            {

                // 2. Query to retrieve results.
                command = new SqlCommand("sp_get_open_balances_pos", connection, transaction);
                command.CommandType = CommandType.StoredProcedure;

                idParam = command.Parameters.Add("@accessCode", SqlDbType.Int);
                idParam.Value = searchCriteria.AccessCode;
                if (searchCriteria.AccessCode == 0)
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40);
                idParam.Value = searchCriteria.Uid;
                if (searchCriteria.Uid == null || string.Empty.Equals(searchCriteria.Uid.Trim()))
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@uidOperator", SqlDbType.NVarChar, 2);
                idParam.Value = searchCriteria.UidOperator;
                if (searchCriteria.UidOperator == null || string.Empty.Equals(searchCriteria.UidOperator.Trim()))
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@userStatus", SqlDbType.NVarChar, 1);
                if (searchCriteria.UserStatus == 0x0000)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = userStatusDbMapper.mapEnumAsString(searchCriteria.UserStatus);
                }

                idParam = command.Parameters.Add("@userType", SqlDbType.NVarChar, 1);
                if (searchCriteria.UserGroup == 0x0000)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = userTypeDbMapper.mapEnumAsString(searchCriteria.UserGroup);
                }

                idParam = command.Parameters.Add("@hideNegative", SqlDbType.NVarChar, 1);
                idParam.Value = searchCriteria.HideNegative;
                if (searchCriteria.HideNegative == null || string.Empty.Equals(searchCriteria.HideNegative.Trim()))
                {
                    idParam.Value = DBNull.Value;
                }

                idParam = command.Parameters.Add("@effdate", SqlDbType.SmallDateTime);
                if (searchCriteria.EffectiveDate.CompareTo(DateTime.MinValue) <= 0)
                {
                    idParam.Value = string.Format("{0:MM/dd/yyyy}", rightNow);
                }
                else
                {
                    idParam.Value = string.Format("{0:MM/dd/yyyy}", searchCriteria.EffectiveDate);
                }

                if (log.IsDebugEnabled)
                {
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {
                        log.Debug(string.Format("{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value));
                    }
                }

                // Retrieve data
                rs = command.ExecuteReader();

                while (rs.Read())
                {
                    int _accessCode = (rs.IsDBNull(rs.GetOrdinal("access_code"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("access_code")));
                    string _notes = (rs.IsDBNull(rs.GetOrdinal("notes"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("notes"))).Trim();
                    string _uid = (rs.IsDBNull(rs.GetOrdinal("uid"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("uid"))).Trim();
                    string _userGroup = (rs.IsDBNull(rs.GetOrdinal("user_group"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("user_group"))).Trim();
                    string _userStatus = (rs.IsDBNull(rs.GetOrdinal("user_status"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("user_status"))).Trim();
                    decimal _amountDue = (rs.IsDBNull(rs.GetOrdinal("amount_due"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amount_due")));
                    string _email = (rs.IsDBNull(rs.GetOrdinal("email"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("email"))).Trim();
                    string _firstName = (rs.IsDBNull(rs.GetOrdinal("first_name"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("first_name"))).Trim();
                    string _lastName = (rs.IsDBNull(rs.GetOrdinal("last_name"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("last_name"))).Trim();
                    string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();
                    DateTime _isEftPendingDate = (rs.IsDBNull(rs.GetOrdinal("isEftPending"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("isEftPending")));

                    //Create new PaymentVO object
                    vo = new OpenBalancePosVO();
                    vo.AccessCode = _accessCode;
                    vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);
                    vo.Notes = _notes;
                    vo.Uid = _uid;
                    vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum(_userGroup);
                    vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum(_userStatus);
                    vo.AmountDue = _amountDue;
                    vo.Email = _email;
                    vo.FirstName = _firstName;
                    vo.LastName = _lastName;
                    vo.IsEftPendingDate = _isEftPendingDate;

                    // Add to ArrayList as a validation check
                    list.Add(vo);

                }

                // Close reader.
                rs.Close();


            }
            catch (Exception e)
            {
                log.Error(e.ToString());
                throw e;
            }

            // Check that only one OpenBalancePosVO object was created.
            if (list.Count > 1)
            {
                log.Error("More than one OpenBalancePosVO was retrieved.");

                // Temporarily clear list.
                list = new ArrayList();
            }

            if (list.Count == 1)
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug("One OpenBalancePosVO retrieved from query and returned.");
                }
                vo = (OpenBalancePosVO)list[0];
                if (log.IsDebugEnabled)
                {
                    log.Debug(string.Format("Retrieved OpenBalancePosVO [{0}].", vo));
                }
            }

            return vo;
        }

        public OpenBalancePosVO retrievePosOpenBalance( int accessCode )
	    {	    		    	    		    	
			if( accessCode <= 0 )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "accessCode is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("accessCode is [{0}]", accessCode) );
			}
	    		    	
			OpenBalancePosVO vo = null;
			
			OpenBalancePosListSearchVO searchCriteria = new OpenBalancePosListSearchVO();
			searchCriteria.AccessCode = accessCode;
			searchCriteria.HideNegative = "N";
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("OpenBalancePosListSearchVO is [{0}]", searchCriteria) );
			}
			
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;
			SqlParameter idParam = null;
			ArrayList list = new ArrayList();
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
			UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
			ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();

            DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();
           
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();

				// 2. Query to retrieve results.
				command = new SqlCommand( "sp_get_open_balances_pos", connection );
				command.CommandType = CommandType.StoredProcedure;				
				
				idParam = command.Parameters.Add("@accessCode", SqlDbType.Int );
				idParam.Value = searchCriteria.AccessCode;				
				if( searchCriteria.AccessCode == 0 )
				{
					idParam.Value = DBNull.Value;
				}				
				
				idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40 );
				idParam.Value = searchCriteria.Uid;				
				if( searchCriteria.Uid == null || string.Empty.Equals( searchCriteria.Uid.Trim() ) )
				{
					idParam.Value = DBNull.Value;
				}
				
				idParam = command.Parameters.Add("@uidOperator", SqlDbType.NVarChar, 2 );
				idParam.Value = searchCriteria.UidOperator;	
				if( searchCriteria.UidOperator == null || string.Empty.Equals( searchCriteria.UidOperator.Trim() ) )
				{
					idParam.Value = DBNull.Value;
				}		
												
				idParam = command.Parameters.Add("@userStatus", SqlDbType.NVarChar, 1 );
				if( searchCriteria.UserStatus == 0x0000 )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = userStatusDbMapper.mapEnumAsString( searchCriteria.UserStatus );
				}		
				
				idParam = command.Parameters.Add("@userType", SqlDbType.NVarChar, 1 );
				if( searchCriteria.UserGroup == 0x0000 )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = userTypeDbMapper.mapEnumAsString( searchCriteria.UserGroup );
				}			
				
				idParam = command.Parameters.Add("@hideNegative", SqlDbType.NVarChar, 1 );
				idParam.Value = searchCriteria.HideNegative;
				if( searchCriteria.HideNegative == null || string.Empty.Equals( searchCriteria.HideNegative.Trim() ) )
				{
					idParam.Value = DBNull.Value;
				}

                idParam = command.Parameters.Add("@effdate", SqlDbType.SmallDateTime);
                if (searchCriteria.EffectiveDate.CompareTo(DateTime.MinValue) <= 0)
                {
                    idParam.Value = string.Format("{0:MM/dd/yyyy}", rightNow);
                }
                else
                {
                    idParam.Value = string.Format("{0:MM/dd/yyyy}", searchCriteria.EffectiveDate);
                }

				if( log.IsDebugEnabled )
				{
				   for (int i = 0; i < command.Parameters.Count; i++) 
				   {
				   	log.Debug( string.Format( "{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value ) );
				   }
				}					
				
				// Retrieve data
				rs = command.ExecuteReader();							

				while( rs.Read() )
				{						
					int _accessCode = ( rs.IsDBNull( rs.GetOrdinal("access_code") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("access_code") ) );
					string _notes = ( rs.IsDBNull( rs.GetOrdinal("notes") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("notes") ) ).Trim();					
					string _uid = ( rs.IsDBNull( rs.GetOrdinal("uid") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("uid") ) ).Trim();
					string _userGroup = ( rs.IsDBNull( rs.GetOrdinal("user_group") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("user_group") ) ).Trim();
					string _userStatus = ( rs.IsDBNull( rs.GetOrdinal("user_status") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("user_status") ) ).Trim();					
					decimal _amountDue = ( rs.IsDBNull( rs.GetOrdinal("amount_due") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amount_due") ) );								
					string _email = ( rs.IsDBNull( rs.GetOrdinal("email") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("email") ) ).Trim();					
					string _firstName = ( rs.IsDBNull( rs.GetOrdinal("first_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("first_name") ) ).Trim();					
					string _lastName = ( rs.IsDBNull( rs.GetOrdinal("last_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("last_name") ) ).Trim();
                    string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();
					DateTime _isEftPendingDate = ( rs.IsDBNull( rs.GetOrdinal("isEftPending") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("isEftPending") ) );
					
					//Create new PaymentVO object
					vo = new OpenBalancePosVO();
					vo.AccessCode = _accessCode;
                    vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);
					vo.Notes = _notes;
					vo.Uid = _uid;
					vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum( _userGroup );
					vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum( _userStatus );
					vo.AmountDue = _amountDue;
					vo.Email = _email;
					vo.FirstName = _firstName;
					vo.LastName = _lastName;
					vo.IsEftPendingDate = _isEftPendingDate;
					
					// Add to ArrayList as a validation check
					list.Add( vo );
										
				}
								
				// Close reader.
				rs.Close();						
				
				connection.Close();

			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw e;
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
			
			// Check that only one OpenBalancePosVO object was created.
			if( list.Count > 1 )
			{
				log.Error( "More than one OpenBalancePosVO was retrieved." );

				// Temporarily clear list.
				list = new ArrayList();
			}
			
			if( list.Count == 1 )
			{
				if( log.IsDebugEnabled )
				{
					log.Debug("One OpenBalancePosVO retrieved from query and returned.");						
				}
				vo = ( OpenBalancePosVO )list[0];
				if(log.IsDebugEnabled)
				{
					log.Debug( string.Format( "Retrieved OpenBalancePosVO [{0}].", vo ) );
				}
			}
			
			return vo;    		
	    }	    		
	    
		public ArrayList retrieveServiceOpenBalancesList( int trackingNumber, UserVO currentUser)
		{	
			OpenBalanceServiceListSearchVO searchCriteria = new OpenBalanceServiceListSearchVO();
			searchCriteria.TrackingNumber = trackingNumber;
			
			return this.retrieveServiceOpenBalancesList( searchCriteria, currentUser);
		}	    
	    
		public ArrayList retrieveServiceOpenBalancesList( int accessCode, string uid, UserVO currentUser)
		{	
			OpenBalanceServiceListSearchVO searchCriteria = new OpenBalanceServiceListSearchVO();
			searchCriteria.AccessCode = accessCode;
			searchCriteria.Uid = uid;
			
			return this.retrieveServiceOpenBalancesList( searchCriteria, currentUser);
		}	    

		public ArrayList retrieveServiceOpenBalancesList( OpenBalanceServiceListSearchVO searchCriteria, UserVO currentUser)
		{			

			if( searchCriteria == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "OpenBalanceServiceListSearchVO [{0}] is null.", searchCriteria );
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("OpenBalanceServiceListSearchVO is [{0}]", searchCriteria) );
			}			
			return retrieveServiceOpenBalancesList(searchCriteria, null, null, currentUser);
        }
        private ArrayList retrieveServiceOpenBalancesList(OpenBalanceServiceListSearchVO searchCriteria, SqlConnection connection, SqlTransaction transaction, UserVO currentUser)
        {
			SqlCommand command = null;
			SqlDataReader rs = null;
			SqlParameter idParam = null;
			ArrayList list = new ArrayList();
			
			ServicePayFrequencyEnumDatabaseMapper servicePayFrequencyDbMapper = ServicePayFrequencyEnumDatabaseMapper.getInstance();
			UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
			UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();
			InvoicePreferenceEnumDatabaseMapper invoicePreferenceEnumDbMapper = InvoicePreferenceEnumDatabaseMapper.getInstance();

            bool useNewConnection = connection == null;

            DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();

            string sqltimeoutstr = ShapeConfiguration.AppSettings("SQLTimeOut");
            int sqltimeout = 240;
            if (!Int32.TryParse(sqltimeoutstr, out sqltimeout))
            {
                sqltimeout = 240;
            }
			try
			{
				// 1. Get the connection string.
				if (useNewConnection)
				{
					string connectionParam = ShapeConnection.retrieveConnection(ConfigurationManager.AppSettings["SQLDBConnString"]);
					// Create and open connection.
					connection = new SqlConnection(connectionParam);
					connection.Open();
					transaction = connection.BeginTransaction();
				}

				// 2. Query to retrieve results.
				command = new SqlCommand("sp_get_service_open_balance", connection, transaction);
				command.CommandTimeout = sqltimeout;
				command.CommandType = CommandType.StoredProcedure;

				idParam = command.Parameters.Add("@effectiveDate", SqlDbType.SmallDateTime);
				idParam.Value = string.Format("{0:MM/dd/yyyy}", searchCriteria.EffectiveDate);
				if (searchCriteria.EffectiveDate.CompareTo(DateTime.MinValue) <= 0)
				{
					idParam.Value = string.Format("{0:MM/dd/yyyy}", rightNow);
				}

				idParam = command.Parameters.Add("@billing_type", SqlDbType.NVarChar, 1);
				idParam.Value = "S";

				idParam = command.Parameters.Add("@access_code", SqlDbType.Int);
				idParam.Value = searchCriteria.AccessCode;
				if (searchCriteria.AccessCode <= 0)
				{
					idParam.Value = DBNull.Value;
				}

				idParam = command.Parameters.Add("@pay_group", SqlDbType.NVarChar, 50);
				idParam.Value = string.Format("{0}", searchCriteria.TrackingNumber);
				if (searchCriteria.TrackingNumber <= 0)
				{
					idParam.Value = DBNull.Value;
				}

				idParam = command.Parameters.Add("@userStatus", SqlDbType.NVarChar, 1);
				idParam.Value = DBNull.Value;
				if (searchCriteria.UserStatus == 0x0000)
				{
					idParam.Value = DBNull.Value;
				}
				else
				{
					idParam.Value = userStatusDbMapper.mapEnumAsString(searchCriteria.UserStatus);
				}

				idParam = command.Parameters.Add("@include_all_services", SqlDbType.Bit);
				idParam.Value = searchCriteria.IncludeAllServices;

				idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40);
				idParam.Value = DBNull.Value;
				if (searchCriteria.Uid != null && !string.Empty.Equals(searchCriteria.Uid.Trim()))
				{
					idParam.Value = searchCriteria.Uid;
				}

				idParam = command.Parameters.Add("@uid_operator", SqlDbType.NVarChar, 2);
				idParam.Value = DBNull.Value;
				if (searchCriteria.UidOperator != null && !string.Empty.Equals(searchCriteria.UidOperator.Trim()))
				{
					idParam.Value = searchCriteria.UidOperator;
				}

				idParam = command.Parameters.Add("@user_group", SqlDbType.NVarChar, 1);
				idParam.Value = DBNull.Value;
				if (searchCriteria.UserGroup != 0x0000)
				{
					idParam.Value = userTypeDbMapper.mapEnumAsString(searchCriteria.UserGroup);
				}

				idParam = command.Parameters.Add("@daysPastDue", SqlDbType.Int);
				idParam.Value = DBNull.Value;
				if (searchCriteria.DaysPastDue > -1)
				{
					idParam.Value = searchCriteria.DaysPastDue;
				}

				idParam = command.Parameters.Add("@user_status_active_and_inactive", SqlDbType.Bit);
				idParam.Value = searchCriteria.UserStatusActiveAndInactive;

				idParam = command.Parameters.Add("@gtid", SqlDbType.NChar, 40);
				idParam.Value = DBNull.Value;
				try
				{
					//UserVO currentUser = (UserVO)System.Web.HttpContext.Current.Session["User"];
					if (currentUser.Type == UserTypeEnum.TRAINER)
					{
						GatewayTrainerDAO gatewayTrainer = new GatewayTrainerDAO();
						GatewayTrainerVO gtVo = gatewayTrainer.retrieveGatewayTrainerSettings(currentUser.Id.Trim());
						if (gtVo != null && !String.IsNullOrEmpty(gtVo.GatewayTraineruserId))
						{
							idParam.Value = gtVo.GatewayTraineruserId.Trim();
						}
					}
				}
				catch (Exception)
				{

				}
				idParam = command.Parameters.Add("@trainerid", SqlDbType.NChar, 40);
				idParam.Value = DBNull.Value;
				try
				{
					//UserVO currentUser = (UserVO)System.Web.HttpContext.Current.Session["User"];
					if (currentUser.Type == UserTypeEnum.TRAINER)
					{
						idParam.Value = currentUser.Id;
					}
				}
				catch (Exception)
				{

				}

				if (log.IsDebugEnabled)
				{
					string sql_proc_str = string.Format("[{0}]", command.CommandText);
					for (int i = 0; i < command.Parameters.Count; i++)
					{
						sql_proc_str = string.Format("{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals(command.Parameters[i].Value) ? "NULL" : string.Format("'{0}'", command.Parameters[i].Value));
					}

					log.Debug(sql_proc_str);
				}

				// Retrieve data
				rs = command.ExecuteReader();

				while (rs.Read())
				{
					string _notesContact = (rs.IsDBNull(rs.GetOrdinal("notesContact"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("notesContact"))).Trim();
					DateTime _lastContact = (rs.IsDBNull(rs.GetOrdinal("lastContact"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("lastContact")));
					string _contactResult = (rs.IsDBNull(rs.GetOrdinal("contactResult"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("contactResult"))).Trim();
					string _contactRep = (rs.IsDBNull(rs.GetOrdinal("contactRep"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("contactRep"))).Trim();
					int _accessCode = (rs.IsDBNull(rs.GetOrdinal("access_code"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("access_code")));
					string _billing_type = (rs.IsDBNull(rs.GetOrdinal("billing_type"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("billing_type"))).Trim();
					string _pay_group = (rs.IsDBNull(rs.GetOrdinal("pay_group"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("pay_group"))).Trim();
					string _uid = (rs.IsDBNull(rs.GetOrdinal("uid"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("uid"))).Trim();
					string _notes = (rs.IsDBNull(rs.GetOrdinal("notes"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("notes"))).Trim();
					decimal _amountExpected = (rs.IsDBNull(rs.GetOrdinal("amountExpected"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amountExpected")));
					decimal _amountPaid = (rs.IsDBNull(rs.GetOrdinal("amountPaid"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amountPaid")));
					decimal _amountDue = (rs.IsDBNull(rs.GetOrdinal("amountDue"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("amountDue")));
					decimal _lateFee = (rs.IsDBNull(rs.GetOrdinal("lateFee"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("lateFee")));
					decimal _serviceFee = (rs.IsDBNull(rs.GetOrdinal("serviceFee"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("serviceFee")));
					decimal _lastPayment = (rs.IsDBNull(rs.GetOrdinal("lastPayment"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("lastPayment")));
					decimal _remainingBalance = (rs.IsDBNull(rs.GetOrdinal("remaining_balance"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("remaining_balance")));
					string _packageId = (rs.IsDBNull(rs.GetOrdinal("package"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("package"))).Trim();
					int _trackingNumber = (rs.IsDBNull(rs.GetOrdinal("trackingNumber"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("trackingNumber")));
					string _payFreq = (rs.IsDBNull(rs.GetOrdinal("payFreq"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("payFreq"))).Trim();
					string _packageNotes = (rs.IsDBNull(rs.GetOrdinal("packageNotes"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("packageNotes"))).Trim();
					//decimal _periodRate = ( rs.IsDBNull( rs.GetOrdinal("periodRate") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("periodRate") ) );					
					DateTime _lastPaymentDate = (rs.IsDBNull(rs.GetOrdinal("lastPaidDate"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("lastPaidDate")));
					DateTime _delinquentDate = (rs.IsDBNull(rs.GetOrdinal("delinquentDate"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("delinquentDate")));

					string _smsGateway = (rs.IsDBNull(rs.GetOrdinal("smsGateway"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("smsGateway"))).Trim();
					bool _smsActiveGateway = (rs.IsDBNull(rs.GetOrdinal("smsGatewayActive"))) ? false : (rs.GetBoolean(rs.GetOrdinal("smsGatewayActive")));
					int _smsGatewayId = (rs.IsDBNull(rs.GetOrdinal("smsGatewayId"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("smsGatewayId")));

					string _userEmail = (rs.IsDBNull(rs.GetOrdinal("userEmail"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userEmail"))).Trim();
					string _userFirstName = (rs.IsDBNull(rs.GetOrdinal("userFirstName"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userFirstName"))).Trim();
					string _userLastName = (rs.IsDBNull(rs.GetOrdinal("userLastName"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userLastName"))).Trim();
					string _userAddress1 = (rs.IsDBNull(rs.GetOrdinal("userAddress1"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userAddress1"))).Trim();
					string _userAddress2 = (rs.IsDBNull(rs.GetOrdinal("userAddress2"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userAddress2"))).Trim();
					string _userCity = (rs.IsDBNull(rs.GetOrdinal("userCity"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userCity"))).Trim();
					string _userState = (rs.IsDBNull(rs.GetOrdinal("userState"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userState"))).Trim();
					string _userZip = (rs.IsDBNull(rs.GetOrdinal("userZip"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userZip"))).Trim();
					string _homePhone = (rs.IsDBNull(rs.GetOrdinal("home_phone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("home_phone"))).Trim();
					string _workPhone = (rs.IsDBNull(rs.GetOrdinal("work_phone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("work_phone"))).Trim();
					string _cellPhone = (rs.IsDBNull(rs.GetOrdinal("cell_phone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("cell_phone"))).Trim();
					string _displayName = (rs.IsDBNull(rs.GetOrdinal("displayName"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("displayName"))).Trim();

					DateTime _isEftPendingDate = (rs.IsDBNull(rs.GetOrdinal("isEftPending"))) ? DateTime.MinValue : (rs.GetDateTime(rs.GetOrdinal("isEftPending")));
					string _userGroup = (rs.IsDBNull(rs.GetOrdinal("userGroup"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userGroup"))).Trim();
					string _userStatus = (rs.IsDBNull(rs.GetOrdinal("userStatus"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("userStatus"))).Trim();
					string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();

					bool _doNotContact = (rs.IsDBNull(rs.GetOrdinal("do_not_contact"))) ? false : (rs.GetBoolean(rs.GetOrdinal("do_not_contact")));
					string _invPrefString = (rs.IsDBNull(rs.GetOrdinal("invoice_pref"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("invoice_pref"))).Trim();

					string _emergencyContactName = (rs.IsDBNull(rs.GetOrdinal("em_contact_name"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("em_contact_name"))).Trim();
					string _emergencyContactPhone = (rs.IsDBNull(rs.GetOrdinal("em_contact_phone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("em_contact_phone"))).Trim();

					if (_uid != null && !string.Empty.Equals(_uid.Trim()))
					{
						ServiceOpenBalancesVO vo = new ServiceOpenBalancesVO(_uid);
						vo.AccessCode = _accessCode;
						vo.DisplayName = string.Format("{0}-{1}", _userLastName, _userFirstName);
						vo.ExpectedPayments = _amountExpected;
						vo.Payments = _amountPaid;
						vo.AmountDue = _amountDue;
						vo.LateFee = _lateFee;
						vo.ServiceFee = _serviceFee;
						vo.LastPayment = _lastPayment;
						vo.RemainingBalance = _remainingBalance;
						vo.PackageId = _packageId;
						vo.TrackingNumber = _trackingNumber;
						vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum(_userGroup);
						vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum(_userStatus);
						vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);

						try
						{
							vo.PayFreq = (ServicePayFrequencyEnum)servicePayFrequencyDbMapper.mapStringAsEnum(string.Format("{0}", _payFreq));
						}
						catch (Exception ex1)
						{
							log.Error(string.Format("_payFreq [{0}] incorrect for [{1}].", _payFreq, _uid), ex1);
						}
						//try
						//{
						//	vo.MembershipStatus = ( MembershipStatusEnum )membershipStatusDbMapper.mapStringAsEnum( string.Format( "{0}", _membershipStatus ) );
						//}
						//catch( Exception ex1 )
						//{
						//	log.Error( string.Format( "_membershipStatus [{0}] incorrect for [{1}].", _membershipStatus, _uid ), ex1 );
						//}						

						vo.LastPaymentDate = _lastPaymentDate;
						//vo.MonthlyRate = _periodRate;
						vo.Notes = _notes;
						vo.NotesContact = _notesContact;
						if (_lastContact != DateTime.MinValue)
						{
							if (String.IsNullOrEmpty(_contactRep))
								vo.NotesContact = string.Format("{0:g}: {1}", _lastContact, _notesContact);
							else
								vo.NotesContact = string.Format("{0:g} by {2}: {1}", _lastContact, _notesContact, _contactRep);
						}
						vo.ContactResult = _contactResult;

						vo.PackageNotes = _packageNotes;
						vo.UserEmail = _userEmail;
						vo.EmergencyContactName = _emergencyContactName;
						vo.EmergencyContactPhone = _emergencyContactPhone;
						vo.MailingLabelData = new MailingLabelData(_userLastName, _userFirstName, _userAddress1, _userAddress2, _userCity, _userState, _userZip);
						vo.MailingLabelData.HomePhone = _homePhone;
						vo.MailingLabelData.WorkPhone = _workPhone;
						vo.MailingLabelData.CellPhone = _cellPhone;
						vo.MailingLabelData.Email = _userEmail;
						vo.MailingLabelData.DoNotContact = _doNotContact;
						vo.DelinquentDate = _delinquentDate;
						vo.IsEftPendingDate = _isEftPendingDate;

						vo.SmsGateway = _smsGateway;
						vo.SmsGatewayActive = _smsActiveGateway;
						vo.SmsGatewayId = _smsGatewayId;


						if ((DateTime.MinValue.CompareTo(_delinquentDate) < 0))
						{
							TimeSpan span = rightNow - _delinquentDate;
							int minutesLeft = (int)(span.Ticks / TimeSpan.TicksPerMinute);
							double daysDelinq = (double)minutesLeft / (double)1440;

							vo.DaysDelinquent = Convert.ToInt32(daysDelinq) - 1;
						}

						if (vo.DaysDelinquent < 0)
						{
							vo.DaysDelinquent = 0;
						}

						vo.DelinquencyLevel = DelinquencyLevelEnum.NONE;

						if (vo.DaysDelinquent >= 1 && vo.DaysDelinquent <= 30)
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.LOW;
						}

						if (vo.DaysDelinquent >= 31 && vo.DaysDelinquent <= 60)
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.MEDIUM;
						}

						if (vo.DaysDelinquent >= 61 && vo.DaysDelinquent <= 90)
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.HIGH;
						}

						if (vo.DaysDelinquent > 90)
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.URGENT;
						}

						if (_invPrefString != null && !string.Empty.Equals(_invPrefString.Trim()))
						{
							try
							{
								vo.InvoicePreference = (InvoicePreferenceEnum)invoicePreferenceEnumDbMapper.mapStringAsEnum(_invPrefString);
							}
							catch (Exception ex12)
							{
								log.Error(string.Format("Incorrect Invoice Preference [{0}] in billing_master", _invPrefString), ex12);
							}
						}

						vo.EffectiveDate = searchCriteria.EffectiveDate;

						log.Debug(vo);

						if (searchCriteria.DelinquencyLevel == 0x0000)
						{
							list.Add(vo);
						}
						else
						{
							if (searchCriteria.DelinquencyLevel.Equals(vo.DelinquencyLevel))
							{
								list.Add(vo);
							}
						}
					}
				}

				// Close reader.
				rs.Close();

				if (useNewConnection)
				{
					transaction.Commit();
                    connection.Close();
                }

			}
			catch (Exception ex)
			{
				string sql_proc_str = string.Empty;
				if (command != null)
				{
					sql_proc_str = string.Format("[{0}]", command.CommandText);
					for (int i = 0; i < command.Parameters.Count; i++)
					{
						sql_proc_str = string.Format("{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals(command.Parameters[i].Value) ? "NULL" : string.Format("'{0}'", command.Parameters[i].Value));
					}
				}

				log.Error(string.Format("Exception thrown running query. [{0}]", sql_proc_str), ex);
				throw ex;
			}
			finally
			{

				if (useNewConnection)
				{
					if (connection != null)
					{
						connection.Close();
						connection = null;
					}
				}
			}
			
			return list;
		}				

	    public ServiceOpenBalancesVO retrieveOpenServiceBalanceForService( int trackingNumber, DateTime effDate, UserVO currentUser)
	    {	    		    	    		    	
			if( trackingNumber <= 0 )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "trackingNumber is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("trackingNumber is [{0}]", trackingNumber) );
			}
	    		    	
			if( effDate.CompareTo( DateTime.MinValue ) <= 0 )
			{
				effDate = DateAndTimeUtil.getLocaleDateTime().Date;
			}					
			
			ServiceOpenBalancesVO vo = null;
			OpenBalanceServiceListSearchVO searchCriteria = new OpenBalanceServiceListSearchVO();
			searchCriteria.TrackingNumber = trackingNumber;										
			searchCriteria.IncludeAllServices = true;
			searchCriteria.EffectiveDate = effDate;
			
			ArrayList list = retrieveServiceOpenBalancesList(searchCriteria, currentUser);
			
			// Check that only one ServiceOpenBalancesVO object was created - should only be one for pay_group
			if( list.Count > 1 )
			{
				log.Error( "More than one ServiceOpenBalancesVO was retrieved." );

				// Temporarily clear list.
				list = new ArrayList();
			}
			
			if( list.Count == 1 )
			{
				if( log.IsDebugEnabled )
				{
					log.Debug("One ServiceOpenBalancesVO retrieved from query and returned.");						
				}
				vo = ( ServiceOpenBalancesVO )list[0];
				if(log.IsDebugEnabled)
				{
					log.Debug( string.Format( "Retrieved ServiceOpenBalancesVO [{0}].", vo ) );
				}
			}
			
			return vo;    		
	    }			
		
	    public ServiceOpenBalancesVO retrieveOpenServiceBalanceForService( int trackingNumber, UserVO currentUser)
	    {	    		    	    		    	
			if( trackingNumber <= 0 )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "trackingNumber is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("trackingNumber is [{0}]", trackingNumber) );
			}
	    		    	
			ServiceOpenBalancesVO vo = null;
			OpenBalanceServiceListSearchVO searchCriteria = new OpenBalanceServiceListSearchVO();
			searchCriteria.TrackingNumber = trackingNumber;										
			searchCriteria.IncludeAllServices = true;

            ArrayList list = retrieveServiceOpenBalancesList(searchCriteria, currentUser);
			
			// Check that only one ServiceOpenBalancesVO object was created - should only be one for pay_group
			if( list.Count > 1 )
			{
				log.Error( "More than one ServiceOpenBalancesVO was retrieved." );

				// Temporarily clear list.
				list = new ArrayList();
			}
			
			if( list.Count == 1 )
			{
				if( log.IsDebugEnabled )
				{
					log.Debug("One ServiceOpenBalancesVO retrieved from query and returned.");						
				}
				vo = ( ServiceOpenBalancesVO )list[0];
				if(log.IsDebugEnabled)
				{
					log.Debug( string.Format( "Retrieved ServiceOpenBalancesVO [{0}].", vo ) );
				}
			}
			
			return vo;    		
	    }

        public ServiceOpenBalancesVO retrieveOpenServiceBalanceForService(int trackingNumber, UserVO currentUser, SqlConnection connection, SqlTransaction transaction)
        {
            if (trackingNumber <= 0)
            {
                // Should throw an exception and log this.
                string msg = string.Format("trackingNumber is not valid.");
                log.Error(msg);
                throw new Exception(msg);
            }

            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("trackingNumber is [{0}]", trackingNumber));
            }

            ServiceOpenBalancesVO vo = null;
            OpenBalanceServiceListSearchVO searchCriteria = new OpenBalanceServiceListSearchVO();
            searchCriteria.TrackingNumber = trackingNumber;
            searchCriteria.IncludeAllServices = true;

            ArrayList list = retrieveServiceOpenBalancesList(searchCriteria, connection, transaction, currentUser);

            // Check that only one ServiceOpenBalancesVO object was created - should only be one for pay_group
            if (list.Count > 1)
            {
                log.Error("More than one ServiceOpenBalancesVO was retrieved.");

                // Temporarily clear list.
                list = new ArrayList();
            }

            if (list.Count == 1)
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug("One ServiceOpenBalancesVO retrieved from query and returned.");
                }
                vo = (ServiceOpenBalancesVO)list[0];
                if (log.IsDebugEnabled)
                {
                    log.Debug(string.Format("Retrieved ServiceOpenBalancesVO [{0}].", vo));
                }
            }

            return vo;
        }
        public ServiceOpenBalancesVO retrieveExternalOpenServiceBalanceForService(int trackingNumber, string fac, UserVO currentUser)
        {
            if (trackingNumber <= 0)
            {
                // Should throw an exception and log this.
                string msg = string.Format("trackingNumber is not valid.");
                log.Error(msg);
                throw new Exception(msg);
            }

            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("trackingNumber is [{0}]", trackingNumber));
            }

            ServiceOpenBalancesVO vo = null;
            OpenBalanceServiceListSearchVO searchCriteria = new OpenBalanceServiceListSearchVO();
            searchCriteria.TrackingNumber = trackingNumber;
            searchCriteria.IncludeAllServices = true;

            ArrayList list = new ArrayList();
            SqlConnection connectionExt = null;
            SqlTransaction transactionExt = null;
            try
            {
                FacilityDAO fDao = new FacilityDAO();
                FacilityVO fVo = fDao.retrieveFacilityVO(fac);
                connectionExt = new SqlConnection(fVo.ConnectionString);
				transactionExt = connectionExt.BeginTransaction();
                list = this.retrieveServiceOpenBalancesList(searchCriteria, connectionExt, transactionExt, currentUser);
				transactionExt.Commit();
            }
            catch (Exception ex)
            {
                connectionExt.Close();
                log.Error("Inner exception in View Cart!", ex);
                throw ex;
            }
            finally
            {
                if (connectionExt != null) connectionExt.Close();
            }

            // Check that only one ServiceOpenBalancesVO object was created - should only be one for pay_group
            if (list.Count > 1)
            {
                log.Error("More than one ServiceOpenBalancesVO was retrieved.");

                // Temporarily clear list.
                list = new ArrayList();
            }

            if (list.Count == 1)
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug("One ServiceOpenBalancesVO retrieved from query and returned.");
                }
                vo = (ServiceOpenBalancesVO)list[0];
                if (log.IsDebugEnabled)
                {
                    log.Debug(string.Format("Retrieved ServiceOpenBalancesVO [{0}].", vo));
                }
            }

            return vo;
        }	 

	    public ThirdPartyBillingOpenBalancesVO retrieveOpenBalanceForThirdPartyBilling( int accessCode )
	    {	    		    	    		    	
			if( accessCode <= 0 )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "accessCode is not valid." );
				log.Error( msg );
				throw new Exception( msg );
			}
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("accessCode is [{0}]", accessCode) );
			}
	    		    	
			ThirdPartyBillingOpenBalancesVO vo = null;
			OpenBalanceThirdPartyBillingListSearchVO searchCriteria = new OpenBalanceThirdPartyBillingListSearchVO();
			searchCriteria.AccessCode = accessCode;										
			
			ArrayList list = retrieveThirdPartyBillingOpenBalancesList( searchCriteria );
			
			// Check that only one ThirdPartyBillingOpenBalancesVO object was created - should only be one for pay_group
			if( list.Count > 1 )
			{
				log.Error( "More than one ThirdPartyBillingOpenBalancesVO was retrieved." );

				// Temporarily clear list.
				list = new ArrayList();
			}
			
			if( list.Count == 1 )
			{
				if( log.IsDebugEnabled )
				{
					log.Debug("One ThirdPartyBillingOpenBalancesVO retrieved from query and returned.");						
				}
				vo = ( ThirdPartyBillingOpenBalancesVO )list[0];
				if(log.IsDebugEnabled)
				{
					log.Debug( string.Format( "Retrieved ThirdPartyBillingOpenBalancesVO [{0}].", vo ) );
				}
			}
			
			return vo;    		
	    }	    
	    
		public ArrayList retrieveThirdPartyBillingOpenBalancesList( OpenBalanceThirdPartyBillingListSearchVO searchCriteria  )
		{			

			if( searchCriteria == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "OpenBalanceThirdPartyBillingListSearchVO [{0}] is null.", searchCriteria );
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("OpenBalanceThirdPartyBillingListSearchVO is [{0}]", searchCriteria) );
			}			
			
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;
			SqlParameter idParam = null;
			ArrayList list = new ArrayList();
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			MembershipPayFrequencyEnumDatabaseMapper payFrequencyDbMapper = MembershipPayFrequencyEnumDatabaseMapper.getInstance();			
			MembershipStatusEnumDatabaseMapper membershipStatusDbMapper = MembershipStatusEnumDatabaseMapper.getInstance();
            AlertLevelEnumDatabaseMapper alertLevelDbMapper = AlertLevelEnumDatabaseMapper.getInstance();
			UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();
			UserTypeEnumDatabaseMapper userTypeDbMapper = UserTypeEnumDatabaseMapper.getInstance();
			InvoicePreferenceEnumDatabaseMapper invoicePreferenceEnumDbMapper = InvoicePreferenceEnumDatabaseMapper.getInstance();
			MembershipSubtypeEnumDatabaseMapper membershipSubtypeDbMapper = MembershipSubtypeEnumDatabaseMapper.getInstance();
			EFTPreferenceEnumDatabaseMapper eftPrefDbMapper = EFTPreferenceEnumDatabaseMapper.getInstance();
			
			DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();

            string sqltimeoutstr = ShapeConfiguration.AppSettings("SQLTimeOut");
            int sqltimeout = 240;
            if (!Int32.TryParse(sqltimeoutstr, out sqltimeout))
            {
                sqltimeout = 240;
            } 
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();			
				
				// 2. Query to retrieve results.
				command = new SqlCommand( "sp_get_tpb_open_balance", connection );
                command.CommandTimeout = sqltimeout;
				command.CommandType = CommandType.StoredProcedure;								
				
				idParam = command.Parameters.Add("@effectiveDate", SqlDbType.SmallDateTime );
				idParam.Value = string.Format( "{0:MM/dd/yyyy}", searchCriteria.EffectiveDate );
				
				idParam = command.Parameters.Add("@billing_type", SqlDbType.NVarChar, 1 );
				idParam.Value = "B";	
				
				idParam = command.Parameters.Add("@access_code", SqlDbType.Int );
				idParam.Value = searchCriteria.AccessCode;
				if( searchCriteria.AccessCode <= 0 )
				{
					idParam.Value = DBNull.Value;
				}				

				idParam = command.Parameters.Add("@pay_group", SqlDbType.NVarChar, 50 );
				idParam.Value = DBNull.Value;
				
				idParam = command.Parameters.Add("@membershipStatus", SqlDbType.NVarChar, 1 );
				if( searchCriteria.MembershipStatus == 0x0000 )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = membershipStatusDbMapper.mapEnumAsString( searchCriteria.MembershipStatus );
				}									
				
				idParam = command.Parameters.Add("@amountDueAmt", SqlDbType.Real );
				idParam.Value = DBNull.Value;
				if( searchCriteria.AmountDueAmt.CompareTo( (decimal)0.00 ) > 0 )
				{
					idParam.Value = searchCriteria.AmountDueAmt;
				}				
				
				idParam = command.Parameters.Add("@daysPastDue", SqlDbType.Int );
				idParam.Value = DBNull.Value;
				if( searchCriteria.DaysPastDue > -1 )
				{
					idParam.Value = searchCriteria.DaysPastDue;
				}					
				
				idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.Cid != null && !string.Empty.Equals( searchCriteria.Cid.Trim() ) )
				{
					idParam.Value = searchCriteria.Cid;				
				}
				
				idParam = command.Parameters.Add("@uid_operator", SqlDbType.NVarChar, 2 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.CidOperator != null && !string.Empty.Equals( searchCriteria.CidOperator.Trim() ) )
				{
					idParam.Value = searchCriteria.CidOperator;	
				}		
												
				idParam = command.Parameters.Add("@user_status", SqlDbType.NVarChar, 1 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.UserStatus != 0x0000 )
				{
					idParam.Value = userStatusDbMapper.mapEnumAsString( searchCriteria.UserStatus );
				}						
												
				idParam = command.Parameters.Add("@user_group", SqlDbType.NVarChar, 1 );
				idParam.Value = DBNull.Value;
				if( searchCriteria.UserGroup != 0x0000 )
				{
					idParam.Value = userTypeDbMapper.mapEnumAsString( searchCriteria.UserGroup );
				}		
				    			
				if (log.IsDebugEnabled)
				{
					string sql_proc_str = string.Format( "[{0}]", command.CommandText );
					for (int i = 0; i < command.Parameters.Count; i++)
					{
						sql_proc_str = string.Format( "{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals( command.Parameters[i].Value ) ? "NULL" : string.Format( "'{0}'", command.Parameters[i].Value ) );
					}
				
					log.Debug( sql_proc_str );
				}
                
				// Retrieve data
				rs = command.ExecuteReader();							

				while( rs.Read() )
				{	
					//billing_type varchar(1), access_code int, pay_group varchar(50), uid varchar(40), notes varchar(512), amountExpected decimal, 
					//amountPaid decimal, amountDue decimal, term char(10) null, payFreq char(1) null, periodRate smallmoney null, lastPaidDate smalldatetime null )'
					int _accessCode = ( rs.IsDBNull( rs.GetOrdinal("access_code") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("access_code") ) );
					string _userStatus = ( rs.IsDBNull( rs.GetOrdinal("user_status") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("user_status") ) ).Trim();					
					string _userGroup = ( rs.IsDBNull( rs.GetOrdinal("userGroup") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userGroup") ) ).Trim();
					string _billing_type = ( rs.IsDBNull( rs.GetOrdinal("billing_type") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("billing_type") ) ).Trim();
					string _pay_group = ( rs.IsDBNull( rs.GetOrdinal("pay_group") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("pay_group") ) ).Trim();
					int _payNum = ( rs.IsDBNull( rs.GetOrdinal("pay_num") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("pay_num") ) );
					string _uid = ( rs.IsDBNull( rs.GetOrdinal("uid") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("uid") ) ).Trim();
					string _notes = ( rs.IsDBNull( rs.GetOrdinal("notes") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("notes") ) ).Trim();
					decimal _amountExpected = ( rs.IsDBNull( rs.GetOrdinal("amountExpected") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountExpected") ) );
					decimal _amountPaid = ( rs.IsDBNull( rs.GetOrdinal("amountPaid") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountPaid") ) );
					decimal _amountDue = ( rs.IsDBNull( rs.GetOrdinal("amountDue") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amountDue") ) );
					string _term = ( rs.IsDBNull( rs.GetOrdinal("term") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("term") ) ).Trim();
					string _payFreq = ( rs.IsDBNull( rs.GetOrdinal("payFreq") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("payFreq") ) ).Trim();
					string _membershipStatus = ( rs.IsDBNull( rs.GetOrdinal("membershipStatus") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("membershipStatus") ) ).Trim();
					DateTime _lastPaymentDate = ( rs.IsDBNull( rs.GetOrdinal("lastPaidDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("lastPaidDate") ) );								    					
					DateTime _delinquentDate = ( rs.IsDBNull( rs.GetOrdinal("delinquentDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("delinquentDate") ) );								    					
					
					string _userEmail = ( rs.IsDBNull( rs.GetOrdinal("userEmail") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userEmail") ) ).Trim();
					string _userFirstName = ( rs.IsDBNull( rs.GetOrdinal("userFirstName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userFirstName") ) ).Trim();
					string _userLastName = ( rs.IsDBNull( rs.GetOrdinal("userLastName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userLastName") ) ).Trim();
					string _userAddress1 = ( rs.IsDBNull( rs.GetOrdinal("userAddress1") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userAddress1") ) ).Trim();
					string _userAddress2 = ( rs.IsDBNull( rs.GetOrdinal("userAddress2") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userAddress2") ) ).Trim();
					string _userCity = ( rs.IsDBNull( rs.GetOrdinal("userCity") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userCity") ) ).Trim();
					string _userState = ( rs.IsDBNull( rs.GetOrdinal("userState") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userState") ) ).Trim();
					string _userZip = ( rs.IsDBNull( rs.GetOrdinal("userZip") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userZip") ) ).Trim();					
					string _displayName = ( rs.IsDBNull( rs.GetOrdinal("displayName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("displayName") ) ).Trim();															
					string _homePhone = ( rs.IsDBNull( rs.GetOrdinal("homePhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("homePhone") ) ).Trim();										
					string _workPhone = ( rs.IsDBNull( rs.GetOrdinal("workPhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("workPhone") ) ).Trim();										
					string _cellPhone = ( rs.IsDBNull( rs.GetOrdinal("cellPhone") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("cellPhone") ) ).Trim();															
                    string _alertLevel = (rs.IsDBNull(rs.GetOrdinal("alert_level"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("alert_level"))).Trim();
					string _invPrefString = ( rs.IsDBNull( rs.GetOrdinal("invoice_pref") ) )  ? string.Empty : ( rs.GetString( rs.GetOrdinal("invoice_pref") ) ).Trim();
					bool _doNotContact = ( rs.IsDBNull( rs.GetOrdinal("do_not_contact") ) ) ? false : ( rs.GetBoolean( rs.GetOrdinal("do_not_contact") ) );
					string _eftPref = ( rs.IsDBNull( rs.GetOrdinal("eftPref") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("eftPref") ) ).Trim();
                    string _smsGateway = (rs.IsDBNull(rs.GetOrdinal("smsGateway"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("smsGateway"))).Trim();
                    bool _smsActiveGateway = (rs.IsDBNull(rs.GetOrdinal("smsGatewayActive"))) ? false : (rs.GetBoolean(rs.GetOrdinal("smsGatewayActive")));
                    int _smsGatewayId = (rs.IsDBNull(rs.GetOrdinal("smsGatewayId"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("smsGatewayId")));
              


					if( _uid != null && !string.Empty.Equals( _uid.Trim() ) )
					{		
						ThirdPartyBillingOpenBalancesVO vo = new ThirdPartyBillingOpenBalancesVO( _uid );
						vo.AccessCode = _accessCode;
						vo.PayNum = _payNum;
						vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum( _userStatus );
						vo.UserGroup = (UserTypeEnum)userTypeDbMapper.mapStringAsEnum( _userGroup );
                        vo.AlertLevel = (AlertLevelEnum)alertLevelDbMapper.mapStringAsEnum(_alertLevel);
						vo.DisplayName = string.Format( "{0}-{1}", _userLastName, _userFirstName );
						vo.ExpectedPayments = _amountExpected;
						vo.Payments = _amountPaid;
						vo.AmountDue = _amountDue;
						//vo.RemainingBalance = _remainingBalance;
						vo.Term = _term;
						try
						{
							vo.PayFreq = ( MembershipPayFrequencyEnum )payFrequencyDbMapper.mapStringAsEnum( string.Format( "{0}", _payFreq ) );
						}
						catch( Exception ex1 )
						{
							log.Error( string.Format( "_payFreq [{0}] incorrect for [{1}].", _payFreq, _uid ), ex1 );
						}
						try
						{
							vo.MembershipStatus = ( MembershipStatusEnum )membershipStatusDbMapper.mapStringAsEnum( string.Format( "{0}", _membershipStatus ) );
						}
						catch( Exception ex1 )
						{
							log.Error( string.Format( "_membershipStatus [{0}] incorrect for [{1}].", _membershipStatus, _uid ), ex1 );
						}						
												
						vo.LastPaymentDate = _lastPaymentDate;
						//vo.MonthlyRate = _periodRate;
						vo.Notes = _notes;
						//vo.EmergencyContactName = _emergencyContactName;
						//vo.EmergencyContactPhone = _emergencyContactPhone;
						vo.UserEmail = _userEmail;
						vo.MailingLabelData = new MailingLabelData( _userLastName, _userFirstName, _userAddress1, _userAddress2, _userCity, _userState, _userZip );	
						vo.MailingLabelData.HomePhone = _homePhone;
						vo.MailingLabelData.WorkPhone = _workPhone;
						vo.MailingLabelData.CellPhone = _cellPhone;
						vo.MailingLabelData.Email = _userEmail;
						vo.MailingLabelData.DoNotContact = _doNotContact;
                        vo.SmsGateway = _smsGateway;
                        vo.SmsGatewayActive = _smsActiveGateway;
                        vo.SmsGatewayId = _smsGatewayId;


						vo.DelinquentDate = _delinquentDate;
						//vo.IsEftPendingDate =  _isEftPendingDate;
						vo.DaysDelinquent = 0;
						if( DateTime.MinValue.CompareTo( _delinquentDate ) < 0 )
						{							
							TimeSpan span = rightNow - _delinquentDate;
							int minutesLeft = (int)(span.Ticks / TimeSpan.TicksPerMinute);
							double daysDelinq =  (double)minutesLeft / (double)1440;		
							
							vo.DaysDelinquent = Convert.ToInt32(daysDelinq) - 1;
						}

						vo.DelinquencyLevel = DelinquencyLevelEnum.NONE;
						
						if( vo.DaysDelinquent >= 1 && vo.DaysDelinquent <= 30 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.LOW;
						}						
						
						if( vo.DaysDelinquent >= 31 && vo.DaysDelinquent <= 60 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.MEDIUM;
						}
						
						if( vo.DaysDelinquent >= 61 && vo.DaysDelinquent <= 90 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.HIGH;
						}
						
						if( vo.DaysDelinquent > 90 )
						{
							vo.DelinquencyLevel = DelinquencyLevelEnum.URGENT;
						}						
						
						if( _invPrefString != null && !string.Empty.Equals( _invPrefString.Trim() ) )
						{
							try
							{
								vo.InvoicePreference = (InvoicePreferenceEnum)invoicePreferenceEnumDbMapper.mapStringAsEnum( _invPrefString );
							}
							catch( Exception ex12 )
							{
								log.Error( string.Format( "Incorrect Invoice Preference [{0}] in billing_master", _invPrefString ), ex12 );
							}
						}	
						
						vo.EftPref = ( string.Empty.Equals( _eftPref.Trim() ) )? 0x0000 : ( EFTPreference )eftPrefDbMapper.mapStringAsEnum( string.Format( "{0}", _eftPref ) );
						

						//vo.MembershipSubType = 0x0000;
						//try
						//{
						//	vo.MembershipSubType = ( MembershipSubtypeEnum )membershipSubtypeDbMapper.mapStringAsEnum( string.Format( "{0}", _membershipSubType ) );
						//}
						//catch( Exception ex1 )
						//{
						//	log.Error( string.Format( "Unable to convert _membershipSubType [{0}] to membershipSubType.", _membershipSubType ), ex1 );
						//}						
						
						vo.EffectiveDate = searchCriteria.EffectiveDate;
						
						//vo.AddOnCountAdvanced = _addOnCountAdvanced;
						//vo.AddOnCountSimple = _addOnCountSimple;
						
						log.Debug( vo );
						
						if( searchCriteria.DelinquencyLevel == 0x0000 )
						{
							list.Add( vo );													
						}
						else
						{
							if( searchCriteria.DelinquencyLevel.Equals( vo.DelinquencyLevel ) )
							{
								list.Add( vo );
							}
						}
					}										
				}
								
				// Close reader.
				rs.Close();								
				
				connection.Close();

			}
			catch( Exception ex )
			{
				string sql_proc_str = string.Empty;
				if( command != null )
				{
					sql_proc_str = string.Format( "[{0}]", command.CommandText );
					for( int i = 0; i < command.Parameters.Count; i++ )
					{
						sql_proc_str = string.Format( "{0}{1} {2}={3}", sql_proc_str, i == 0 ? string.Empty : ",", command.Parameters[i].ParameterName, DBNull.Value.Equals( command.Parameters[i].Value ) ? "NULL" : string.Format( "'{0}'", command.Parameters[i].Value ) );
					}
				}
				
				log.Error( string.Format( "Exception thrown running query. [{0}]", sql_proc_str ), ex );
				throw ex;
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
			
			return list;
		}				
		    
	    
	    
		public ArrayList convertOpenBalancesTrainingPackages_NOTUSED( UserVO user )
		{
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("About to convert the open balance list for training packages." ) );
			}													
			
			ArrayList list = new ArrayList();
			ArrayList summaryList = new ArrayList();
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;					
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();

				// 2. Query to retrieve the user.
				command = new SqlCommand( "sp_get_open_balances_training_packages_conversion", connection );
				command.CommandType = CommandType.StoredProcedure;										
																
				// Retrieve data
				rs = command.ExecuteReader();			
								
				while( rs.Read() )
				{
					int _trackingNumber = ( rs.IsDBNull( rs.GetOrdinal("trackingNumber") ) )  ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("trackingNumber") ) );
					int _accessCode = ( rs.IsDBNull( rs.GetOrdinal("access_code") ) )  ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("access_code") ) );
					string _userid = ( rs.IsDBNull( rs.GetOrdinal("userid") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userid") ) ).Trim();
					string _userFirstName = ( rs.IsDBNull( rs.GetOrdinal("userFirstName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userFirstName") ) ).Trim();
					string _userLastName = ( rs.IsDBNull( rs.GetOrdinal("userLastName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userLastName") ) ).Trim();
					DateTime _recordDate = ( rs.IsDBNull( rs.GetOrdinal("recordDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("recordDate") ) );
					double _totalAmount = ( rs.IsDBNull( rs.GetOrdinal("totalAmount") ) )  ? (double)0.00 : ( rs.GetFloat( rs.GetOrdinal("totalAmount") ) );
					DateTime _startDate = ( rs.IsDBNull( rs.GetOrdinal("startDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("startDate") ) );
					double _balance = ( rs.IsDBNull( rs.GetOrdinal("balance") ) )  ? (double)0.00 : ( rs.GetFloat( rs.GetOrdinal("balance") ) );
					string _action = ( rs.IsDBNull( rs.GetOrdinal("action") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("action") ) ).Trim();
					DateTime _effectiveDate = ( rs.IsDBNull( rs.GetOrdinal("effectiveDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("effectiveDate") ) );
					string _payMethod = ( rs.IsDBNull( rs.GetOrdinal("payMethod") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("payMethod") ) ).Trim();
					short _months = ( rs.IsDBNull( rs.GetOrdinal("months") ) )  ? (short)0 : ( rs.GetInt16( rs.GetOrdinal("months") ) );
					short _days = ( rs.IsDBNull( rs.GetOrdinal("days") ) )  ? (short)0 : ( rs.GetInt16( rs.GetOrdinal("days") ) );
					double _payments = ( rs.IsDBNull( rs.GetOrdinal("payments") ) )  ? (double)0.00 : ( rs.GetFloat( rs.GetOrdinal("payments") ) );					
					string _packageName = ( rs.IsDBNull( rs.GetOrdinal("packageName") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("packageName") ) ).Trim();
					DateTime _lastPayDate = ( rs.IsDBNull( rs.GetOrdinal("lastPayDate") ) ) ? DateTime.MinValue : ( rs.GetDateTime( rs.GetOrdinal("lastPayDate") ) );
					string _userNote = ( rs.IsDBNull( rs.GetOrdinal("userNote") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("userNote") ) ).Trim();
					string _packageNote = ( rs.IsDBNull( rs.GetOrdinal("packageNote") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("packageNote") ) ).Trim();
					double _initFee = ( rs.IsDBNull( rs.GetOrdinal("initFee") ) )  ? (double)0.00 : ( rs.GetFloat( rs.GetOrdinal("initFee") ) );
					
					if( !"M".Equals( _payMethod ) )
					{
						_months = (short)0;
						_days = (short)0;
					}					
					
					
					AllSessionHistoryForBalanceVO vo = new AllSessionHistoryForBalanceVO( _trackingNumber, _userid, string.Format( "{0}-{1}", _userLastName, _userFirstName ), _recordDate, _totalAmount,_startDate,
							_balance, _action, _effectiveDate, _payMethod, _months, _days, _payments, _packageName, _lastPayDate, _userNote, _packageNote, _initFee );
					
					vo.AccessCode = _accessCode;
					
					log.Debug( vo.ToString() );
					
					// Add to ArrayList as a validation check
					list.Add( vo );				
				}
				
				// Close reader.
				rs.Close();								
				connection.Close();

			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw new Exception();
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				

			int firstMonthFactor = 2;
			
			// NOTE -- firstMonthsBalanceFactor should be accessed through PackageTypeVO or TrainingPackageVO.
			// 			This wasn't updated since this function is no longer used.
//			string fMonthFactor = ConfigurationManager.AppSettings["firstMonthsBalanceFactor"];
//			try
//			{				
//				if( fMonthFactor != null && !string.Empty.Equals( fMonthFactor.Trim() ) )
//				{
//					firstMonthFactor = Convert.ToInt32( fMonthFactor );
//				}
//			}
//			catch( Exception fmfEx )
//			{
//				string msg = string.Format( "Error converting firstMonthsBalanceFactor to int32 [{0}]", fMonthFactor );
//				log.Error( msg, fmfEx );
//				throw new Exception( msg );				
//			}
			
			AllSessionHistoryForBalanceVO prevDetail = null;
			SessionBalanceVO summaryVO = null;
			DateTime freezeDate = DateTime.MinValue;
			DateTime unfreezeDate = DateTime.MinValue;
			SortedList _freezeTable = new SortedList();
			TimeSpan freezeRange = new TimeSpan((long)0);
			SortedList expectedBalanceTable = new SortedList();
			SortedList actionTable = new SortedList();
			
			DateTime rightNow = DateAndTimeUtil.getLocaleDateTime();
			DateTime todaysDate = new DateTime( rightNow.Year, rightNow.Month, rightNow.Day );
			
			//Loop through 
			IEnumerator ie = list.GetEnumerator();
			while ( ie.MoveNext() )
			{
				AllSessionHistoryForBalanceVO detail = (AllSessionHistoryForBalanceVO)ie.Current;	
				log.Debug( string.Format("Next item: {0}", detail.TrackingNumber ));
						
				// Are we on the same session?
				if( prevDetail!= null && prevDetail.TrackingNumber == detail.TrackingNumber )
				{
					if( log.IsDebugEnabled )
					{
						log.Debug( string.Format( "Same session: {0}", detail ) );
					}
					
					// Still on same session, check for new package price or length.
					if( prevDetail.TotalAmount.CompareTo( detail.TotalAmount ) != 0 || prevDetail.Months != detail.Months || prevDetail.Days != detail.Days 
					   || "Freeze".Equals( detail.Action ) || "Release Freeze".Equals( detail.Action ) )					  
					{
						if( "Freeze".Equals( detail.Action ) )
						{
							//goto next loop
							freezeDate = detail.EffectiveDate;
							continue;
						}
						
						if( "Release Freeze".Equals( detail.Action ) )
						{
							unfreezeDate = detail.EffectiveDate;
							// calc the freeze range and add it to the range only if a matching Freeze is found.	
							if( DateTime.MinValue.CompareTo( freezeDate ) >= 0 )
							{
								freezeDate = detail.EffectiveDate;
							}
							
							if( unfreezeDate.CompareTo( freezeDate ) >= 0 )
							{
								freezeRange = freezeRange.Add( unfreezeDate.Subtract( freezeDate ) );
							}
							else
							{
								freezeRange = freezeRange.Add( new TimeSpan(0) );
							}
							
							if( !_freezeTable.ContainsKey( freezeDate ) )
							{
								_freezeTable.Add( freezeDate, unfreezeDate );
							}
							else
							{
								_freezeTable.SetByIndex( _freezeTable.IndexOfKey( freezeDate ), unfreezeDate );
							}
							
							continue;
						}																						
						
						// calculate new rate with new package length and price
						double newMonthlyRate = (double)0.00;
						int months = detail.Months;
						int days = detail.Days;
						int daysAsMonths = days / 30;  //should get truncated:  less than 30 days == 0; 30 to 59 == 1; etc 
						
						log.Debug( string.Format( "daysAsMonths [{0}]", daysAsMonths ) );						
						
						// add days converted to months to the months value.
						months = months + daysAsMonths;
						
						// If the number of additional days into 30 is more than 15, bump month up one.
						log.Debug( string.Format( "days % 30 [{0}]", days % 30 ) );
						if( ( days - ( daysAsMonths * 30 ) ) >= 15 )
						{
							months++;
						}
																									
						// Just in case the days and/or months weren't setup correctly
						if( months <= 0 )
						{
							months = 1;
						}						
						
						//if( !"M".Equals( detail.PayMethod ) )
						//{
						//	months = 1;
						//}
						
						// Monthly rate				
						newMonthlyRate = ( detail.TotalAmount - (detail.InitFee + detail.InitFee2 + detail.InitFee3 + detail.InitFee4 + detail.InitFee5) ) / months;
						
						log.Debug( string.Format("newMonthlyRate [{0}]", newMonthlyRate ) );
											
						//if length change, we need to swap to new table with correct size
						if( prevDetail.Months != detail.Months || prevDetail.Days != detail.Days )
						{
							if( expectedBalanceTable.Count > months )
							{
								int currentCount = expectedBalanceTable.Count;  //zero index
								
								//packaged shortened - truncate table
								while( currentCount > months )
								{
									expectedBalanceTable.RemoveAt( --currentCount );																		
								}
								
							}
							else if ( expectedBalanceTable.Count < months )
							{
								int currentCount = expectedBalanceTable.Count;  //zero index
								
								//packaged lengthened increase table size
								for( int i = currentCount; i < months; i++ )
								{
									expectedBalanceTable.Add( ((DateTime)expectedBalanceTable.GetKey(0)).AddMonths( i ), newMonthlyRate );
								}
							}
							else
							{
								// stayed the same - do nothing
							}
								
							if( log.IsDebugEnabled )
							{
								for( int i = 0; i < expectedBalanceTable.Count; i++ )
							    {
							    	log.Debug( string.Format( "\t{0}:\t{1:c}", (DateTime)expectedBalanceTable.GetKey( i ), (double)expectedBalanceTable.GetByIndex( i ) ) );
							    }
							}
							
						}
					
						
						if( detail.Action != null && detail.Action.StartsWith("Price:") )
						{
							log.Debug( string.Format("in price change" ) );
							          
							// Price Change
							DateTime lastChangeDate = DateTime.MinValue;

							for( int i = 0; i < actionTable.Count; i++)
							{
								if( detail.EffectiveDate.CompareTo( (DateTime)actionTable.GetKey( i ) ) > 0 )
								{	
									lastChangeDate = (DateTime)actionTable.GetKey( i );									
								}									
							}								
							
							if( log.IsDebugEnabled )
							{
								log.Debug( string.Format("Last Change Date [{0}]", lastChangeDate ) );
							}
							
							DateTime shortLastChangeDate = new DateTime( lastChangeDate.Year, lastChangeDate.Month, lastChangeDate.Day );							
							
							for( int i = 0; i < expectedBalanceTable.Count; i++)
							{
								// Update table with new rate from price and months starting at lastChangeDate
								if( shortLastChangeDate.CompareTo( (DateTime)expectedBalanceTable.GetKey( i ) ) <= 0 )
								{	
									//Update table with new monthly rates
									expectedBalanceTable.SetByIndex( i, ( (i == 0 && "M".Equals( detail.PayMethod ) ) ? newMonthlyRate * firstMonthFactor : newMonthlyRate ) );
								}	
								
								if( log.IsDebugEnabled )
								{
									log.Debug( string.Format( "\t{0}:\t{1:c}", (DateTime)expectedBalanceTable.GetKey( i ), (double)expectedBalanceTable.GetByIndex( i ) ) );
								}
							}																
						}
						else
						{
							
							// Package Change, update sortedlist with new values from the record date forward
							DateTime shortRecordDate = new DateTime( detail.EffectiveDate.Year, detail.EffectiveDate.Month, detail.EffectiveDate.Day );
							
							log.Debug( string.Format("****Package Change Date [{0}]", shortRecordDate ) );
							log.Debug( string.Format("****expectedBalanceTable.Count [{0}]", expectedBalanceTable.Count ) );
							
							if( expectedBalanceTable.Count >= 1 && detail.Action.Trim().IndexOf( ": chg" ) > 0 )
							{
								DateTime openDate = (DateTime)expectedBalanceTable.GetKey( 0 );
							    log.Debug( string.Format("****openDate: [{0}]", openDate ) );  
							    							    
							    if( shortRecordDate.CompareTo( ((DateTime)expectedBalanceTable.GetKey( 0 ) ).AddMonths(1) ) >= 0 )
								{	
							    	log.Debug( string.Format("****actionTable.ContainsKey( detail.EffectiveDate ) [{0}]", actionTable.ContainsKey( detail.EffectiveDate ) ) );
									if( !actionTable.ContainsKey( detail.EffectiveDate ) )
									{
										actionTable.Add( detail.EffectiveDate, detail.Action );
									}
								}
							}
							
							for ( int j = 0; j < expectedBalanceTable.Count; j++ )  
							{							 
								DateTime openDate = (DateTime)expectedBalanceTable.GetKey( 0 );
							    log.Debug( string.Format("****openDate: [{0}]", openDate ) );								
							
								if( j == 0 && expectedBalanceTable.Count >= 1 )
								{																		
									// Reload table fresh - change occurred in first month
									// Same Action for both Normal and Monthly packages
									if( shortRecordDate.CompareTo( (openDate).AddMonths(1) ) < 0 )
									{								
										log.Debug( string.Format(" in openDate [{0}]", openDate ) );
										expectedBalanceTable = new SortedList();
										if( "M".Equals( detail.PayMethod ) )
										{
											for( int i = 0; i < months; i++ )
											{
												expectedBalanceTable.Add( detail.StartDate.AddMonths( i ), ( (i == 0) ? newMonthlyRate * firstMonthFactor : newMonthlyRate ) );
												
												if( log.IsDebugEnabled )
												{
													log.Debug( string.Format( "\t{0}:\t{1:c}", (DateTime)expectedBalanceTable.GetKey( i ), (double)expectedBalanceTable.GetByIndex( i ) ) );
												}
											}
										}
										else
										{
											expectedBalanceTable.Add( detail.StartDate, detail.TotalAmount );
										}
																				
										break;
									}
									else
									{
										// Change occurred after one month																																								
										if( !"M".Equals( prevDetail.PayMethod ) && !"M".Equals( detail.PayMethod ) )
										{
											// Normal to Normal - rewrite table
											expectedBalanceTable = new SortedList();
											expectedBalanceTable.Add( detail.StartDate, detail.TotalAmount );
											break;
										}
										
										if( "M".Equals( prevDetail.PayMethod ) && !"M".Equals( detail.PayMethod ) )
										{
											// Monthly to Normal - rewrite table
											expectedBalanceTable = new SortedList();
											expectedBalanceTable.Add( detail.StartDate, detail.TotalAmount );
											break;
										}										
										
										if( !"M".Equals( prevDetail.PayMethod ) && "M".Equals( detail.PayMethod ) )
										{
											// Normal to Monthly - rewrite table
											expectedBalanceTable = new SortedList();
											for( int i = 0; i < months; i++ )
											{
												expectedBalanceTable.Add( detail.StartDate.AddMonths( i ), ( (i == 0) ? newMonthlyRate * firstMonthFactor : newMonthlyRate ) );
												
												if( log.IsDebugEnabled )
												{
													log.Debug( string.Format( "\t{0}:\t{1:c}", (DateTime)expectedBalanceTable.GetKey( i ), (double)expectedBalanceTable.GetByIndex( i ) ) );
												}
											}
											break;
										}																							
									}
																	
								}
								else
								{					
									// ONLY end up here if Monthly to Monthly - adjust based on date
									// May Need to replace only certain items in this sorted list
									log.Debug( string.Format("***(DateTime)expectedBalanceTable.GetKey( j ) [{0}]", (DateTime)expectedBalanceTable.GetKey( j ) ) );
									if( shortRecordDate.CompareTo( (DateTime)expectedBalanceTable.GetKey( j ) ) <= 0 )
									{	
										//Update table with new monthly rates
										expectedBalanceTable.SetByIndex( j, newMonthlyRate );
									}
											
									if( log.IsDebugEnabled )
									{									
										log.Debug( string.Format( "\t{0}:\t{1:c}", (DateTime)expectedBalanceTable.GetKey( j ), (double)expectedBalanceTable.GetByIndex( j ) ) );									
									}
								}
	
							}
						}
						
						if( detail.Action != null && ( "Open".Equals( detail.Action.Trim() ) || "Open Auto".Equals( detail.Action.Trim() )  ) )
						{
							if( !actionTable.ContainsKey( detail.EffectiveDate ) )
							{							
								actionTable.Add( detail.EffectiveDate, detail.Action );
							}
						}						
																					
					}
					
					
				}
				else
				{
					
					if( prevDetail != null )
					{					
						if( log.IsDebugEnabled )
						{
							log.Debug( string.Format( "Creating Payments for {0}", prevDetail ) );
						}	
						
						//##############
						
						if( _freezeTable.Count > 0 )
						{
							foreach ( DictionaryEntry myDE in expectedBalanceTable )
							{																				
								log.Debug( string.Format( "BEFORE FREEZE: \t{0}:\t{1:c}", (DateTime)myDE.Key, (double)myDE.Value ) );														
							}		
						}
						
						// Apply freeze Table
						foreach ( DictionaryEntry ft in _freezeTable )
						{						
							// load temp table
							SortedList expectedBalanceTableTemp = new SortedList();
							
							log.Debug( string.Format( "FREEZE TABLE: \t{0}:\t{1}", (DateTime)ft.Key, (DateTime)ft.Value ) );							
							TimeSpan currentFreezeRange = ((DateTime)ft.Value).Subtract( (DateTime)ft.Key );
							log.Debug( string.Format( "currentFreezeRange: {0}", currentFreezeRange ) );
																																		
							foreach ( DictionaryEntry myDE in expectedBalanceTable )
							{												
								if( ((DateTime)myDE.Key).CompareTo( (DateTime)ft.Key ) >= 0 )
								{								
									// if the freeze date is greater than or equal to the payment date, 
									// then increase dates forward by currentFreezeRange
									expectedBalanceTableTemp.Add( ((DateTime)myDE.Key).Add( currentFreezeRange ), (double)myDE.Value );
								}
								else
								{
									// Save current record - no changes necessary
									expectedBalanceTableTemp.Add( (DateTime)myDE.Key, (double)myDE.Value );
								}
							}
							
							// Reset to permanent table
							expectedBalanceTable = expectedBalanceTableTemp;
						}							
									
						if( _freezeTable.Count > 0 )
						{						
							foreach ( DictionaryEntry myDE in expectedBalanceTable )
							{																				
								log.Debug( string.Format( "AFTER FREEZE: \t{0}:\t{1:c}", (DateTime)myDE.Key, (double)myDE.Value ) );														
							}								
						}
						//##############
												
						ArrayList _payments = new ArrayList();
						
						// Adjust payment count
						int numberPayments = expectedBalanceTable.Count;
						if( !"M".Equals( prevDetail.PayMethod ) )
						{
							numberPayments = 1;
							log.Debug( string.Format( "numberPayments: [{0}]", numberPayments ) );

							// Sum up all payments from table
							double rt = (double)0;
							foreach ( DictionaryEntry myDE in expectedBalanceTable )
							{									
								rt = rt + (double)myDE.Value;																							
							}															
							
							PaymentVO vo = new PaymentVO();
							vo.AccessCode = prevDetail.AccessCode;
							vo.BillingType = "S";
							vo.PaymentGroup = string.Format("{0}", prevDetail.TrackingNumber ); //analogous to membership id / session id
							vo.PaymentNumber = 1;														
							
							vo.PaymentDueDate = prevDetail.StartDate;
							
							vo.RateAmount = (decimal)rt;			
							double iniamt = (double)prevDetail.InitFee;
                            vo.InitAmount = (decimal)iniamt;
                            double ini2amt = (double)prevDetail.InitFee2;
                            vo.Init2Amount = (decimal)ini2amt;
                            double ini3amt = (double)prevDetail.InitFee3;
                            vo.Init3Amount = (decimal)ini3amt;
                            double ini4amt = (double)prevDetail.InitFee4;
                            vo.Init4Amount = (decimal)ini4amt;
                            double ini5amt = (double)prevDetail.InitFee5;
                            vo.Init5Amount = (decimal)ini5amt;
							vo.AccrualAmount = (decimal)0;
                            vo.ExtraChargeAmount = (decimal)0;
                            vo.ExtraChargeAmount2 = (decimal)0;
                            vo.ExtraChargeAmount3 = (decimal)0;
                            vo.ExtraChargeAmount4 = (decimal)0;
                            vo.ExtraChargeAmount5 = (decimal)0;
							vo.CreditAmt = (decimal)0;
							vo.PaidAmount = (decimal)0;		
							
							_payments.Add( vo );							
						}
						else
						{
							numberPayments = numberPayments - ( firstMonthFactor - 1 );
							if( numberPayments <= 0 )
							{
								numberPayments = 1;
							}						
							log.Debug( string.Format( "numberPayments: [{0}]", numberPayments ) );
														
							int i = 0;
							foreach ( DictionaryEntry myDE in expectedBalanceTable )
							{	
								i++;
								if( i <= numberPayments )
								{
									PaymentVO vo = new PaymentVO();
									vo.AccessCode = prevDetail.AccessCode;
									vo.BillingType = "S";
									vo.PaymentGroup = string.Format("{0}", prevDetail.TrackingNumber ); //analogous to membership id / session id
									vo.PaymentNumber = i;														
									
									vo.PaymentDueDate = (DateTime)myDE.Key;
									
									double rt = (double)myDE.Value;
									
									vo.RateAmount = (decimal)rt;
									vo.InitAmount = (decimal)0;					
									if( i == 1 )
									{
										double iniamt = (double)prevDetail.InitFee;
										vo.InitAmount = (decimal)iniamt;
                                    }
                                    vo.Init2Amount = (decimal)0;
                                    if (i == 1)
                                    {
                                        double ini2amt = (double)prevDetail.InitFee2;
                                        vo.Init2Amount = (decimal)ini2amt;
                                    }
                                    vo.Init3Amount = (decimal)0;
                                    if (i == 1)
                                    {
                                        double ini3amt = (double)prevDetail.InitFee3;
                                        vo.Init3Amount = (decimal)ini3amt;
                                    }
                                    vo.Init4Amount = (decimal)0;
                                    if (i == 1)
                                    {
                                        double ini4amt = (double)prevDetail.InitFee4;
                                        vo.Init4Amount = (decimal)ini4amt;
                                    }
                                    vo.Init5Amount = (decimal)0;
                                    if (i == 1)
                                    {
                                        double ini5amt = (double)prevDetail.InitFee5;
                                        vo.Init5Amount = (decimal)ini5amt;
                                    }
									vo.AccrualAmount = (decimal)0;
                                    vo.ExtraChargeAmount = (decimal)0;
                                    vo.ExtraChargeAmount2 = (decimal)0;
                                    vo.ExtraChargeAmount3 = (decimal)0;
                                    vo.ExtraChargeAmount4 = (decimal)0;
                                    vo.ExtraChargeAmount5 = (decimal)0;
									vo.CreditAmt = (decimal)0;
									vo.PaidAmount = (decimal)0;		
									
									_payments.Add( vo );
								}
							}		
						}
						
						decimal _servicePrice = (decimal)0;
						foreach( PaymentVO pv in _payments )
						{
							log.Debug( pv );
                            _servicePrice = _servicePrice + pv.RateAmount + pv.InitAmount + pv.Init2Amount + pv.Init3Amount + pv.Init4Amount + pv.Init5Amount + pv.ExtraChargeAmount + pv.ExtraChargeAmount2 + pv.ExtraChargeAmount3 + pv.ExtraChargeAmount4 + pv.ExtraChargeAmount5 + pv.AccrualAmount;
						}
						
						log.Debug( string.Format( "_servicePrice [{0}]", _servicePrice ) );						
						
				        UserDAO udao = new UserDAO();
				        UserBaseVO uvo = udao.retrieveUserVOByAccessCode( prevDetail.AccessCode );						
										        				        
						SqlConnection connection2 = new SqlConnection( ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] ) );				
						connection2.Open();
						SqlTransaction transaction2 = connection2.BeginTransaction();			
						int returnValue = -1;
						try
						{																
							
							TrainingPackageDAO tpdao = new TrainingPackageDAO();
							returnValue = tpdao.insertConvertedScheduledPayments( _payments, prevDetail.AccessCode, string.Format("{0}", prevDetail.TrackingNumber ), (decimal)prevDetail.Payments, connection2, transaction2, user);
									
							if( returnValue < 0 )
							{
								Auditor auditor = new Auditor( user );
								AuditVO avo = new AuditVO();
								avo.Function = AuditFunctions.CONVERSION.ToString();
								avo.SubFunction = AuditSubFunctionsConversion.SERVICES_FAILURE.ToString();
								avo.PreImage = string.Format("Service [{0}-{1}] for {2} failed conversion: schedPaymentsCount[{3}], amount[{4:C}], totalPaid[{5:C}]", prevDetail.TrackingNumber, prevDetail.PackageName, prevDetail.DisplayName, numberPayments, _servicePrice, prevDetail.Payments );
								avo.ActionOnUser = uvo.DisplayName;
								avo.ActionOnUserGroup = uvo.UserGroup;								
								auditor.audit( avo );
								throw new Exception();
							}
							else
							{
								Auditor auditor = new Auditor( user );
								AuditVO avo = new AuditVO();
								avo.Function = AuditFunctions.CONVERSION.ToString();
								avo.SubFunction = AuditSubFunctionsConversion.SERVICES_SUCCESS.ToString();
								avo.PreImage = string.Format("Service [{0}-{1}] for {2} converted to payment table: schedPaymentsCount[{3}], amount[{4:C}], totalPaid[{5:C}]", prevDetail.TrackingNumber, prevDetail.PackageName, prevDetail.DisplayName, numberPayments, _servicePrice, prevDetail.Payments );
								avo.ActionOnUser = uvo.DisplayName;
								avo.ActionOnUserGroup = uvo.UserGroup;
								
								auditor.audit( avo, connection2, transaction2 );
							}							
							
							// Everything good - commit this transaction
							transaction2.Commit();
							
							// Close this connection
							connection2.Close();					
						}		
						catch( Exception tranE )
						{
							transaction2.Rollback();
							string msg = string.Format( "Adding scheduled service payments failed: [{0}-{1}] for {2}", prevDetail.TrackingNumber, prevDetail.PackageName, prevDetail.DisplayName );
							log.Error( msg, tranE );
							throw new Exception( msg, tranE );
							// keep processing.
						}		
						finally
						{				
							if ( connection2 != null )			
							{
								connection2.Close();
								connection2 = null;
							}			
						}						
						
						
						
						
						
						
						
						
						// update summaryVO with expected balance and add it to the array list
						// first sum all of the amounts in the table with pay dates less than 
						// or equal to today				
						foreach ( DictionaryEntry myDE in expectedBalanceTable )
						{																				
							log.Debug( string.Format( "\t{0}:\t{1:c}", (DateTime)myDE.Key, (double)myDE.Value ) );
														
							if( todaysDate.CompareTo( ((DateTime)myDE.Key).Add( freezeRange ) ) >= 0 )
							{
								if( expectedBalanceTable.IndexOfKey( myDE.Key ) <= ( expectedBalanceTable.Count - firstMonthFactor ) )
								{
									
									summaryVO.TotalAmountDue = summaryVO.TotalAmountDue + (double)myDE.Value;
									
									
								}																
							}
						}						
						
						if( !"M".Equals( prevDetail.PayMethod ) )
						{
							summaryVO.TotalAmountDue = prevDetail.TotalAmount - summaryVO.Payments;
						}
						else
						{
                            summaryVO.TotalAmountDue = summaryVO.TotalAmountDue - summaryVO.Payments + prevDetail.InitFee + prevDetail.InitFee2 + prevDetail.InitFee3 + prevDetail.InitFee4 + prevDetail.InitFee5;
						}												
						
						log.Debug( string.Format( "Total Amount is: {0}", summaryVO.TotalAmountDue ) );
						if( summaryVO.TotalAmountDue.CompareTo( (double)0.01 ) >= 0 )
						{											
							summaryList.Add( summaryVO );
						}
					}
					
					// Reload expectedBalanceTable with new values
					// Package length based on open.
					double monthlyRate = (double)0.00;					
					int months = detail.Months;
					int days = detail.Days;
					int daysAsMonths = days / 30;  //should get truncated:  less than 30 days == 0; 30 to 59 == 1; etc 
					
					log.Debug( string.Format( "daysAsMonths [{0}]", daysAsMonths ) );						
					
					// add days converted to months to the months value.
					months = months + daysAsMonths;
					
					// If the number of additional days into 30 is more than 15, bump month up one.
					log.Debug( string.Format( "days % 30 [{0}]", days % 30 ) );
					if( ( days - ( daysAsMonths * 30 ) ) >= 15 )
					{
						months++;
					}
					
					// Just in case the days and/or months weren't setup correctly
					if( months <= 0 )
					{
						months = 1;
					}						
					
					// Monthly rate									
					monthlyRate = ( detail.TotalAmount - (detail.InitFee + detail.InitFee2 + detail.InitFee3 + detail.InitFee4 + detail.InitFee5) ) / months;

					log.Debug( string.Format("detail.TotalAmount [{0}]", detail.TotalAmount ) );
					log.Debug( string.Format("initFee [{0}]", detail.InitFee ) );
					log.Debug( string.Format("months [{0}]", months ) );					
					log.Debug( string.Format("monthlyRate [{0}]", monthlyRate ) );
					
					// Initiate and load action table					
					actionTable = new SortedList();
					if( detail.Action != null && ( "Open".Equals( detail.Action.Trim() ) || "Open Auto".Equals( detail.Action.Trim() ) || detail.Action.Trim().IndexOf( ": chg" ) > 0 ) )
					{
						if( !actionTable.ContainsKey( detail.EffectiveDate ) )
						{						
							actionTable.Add( detail.EffectiveDate, detail.Action );
						}
					}
					
					//Populate new expectedBalanceTable hashtable for next tracking number
					expectedBalanceTable = new SortedList();
					for( int i = 0; i < months; i++ )
					{
						if( (i == 0 && "M".Equals( detail.PayMethod ) ) )
						{
							expectedBalanceTable.Add( detail.StartDate.AddMonths( i ), monthlyRate * firstMonthFactor );
						}
						else
						{
							expectedBalanceTable.Add( detail.StartDate.AddMonths( i ), monthlyRate );
						}
					}
					
					// Create new summary
					summaryVO = new SessionBalanceVO( detail.TrackingNumber, detail.Userid, detail.DisplayName, (double)0.00, detail.StartDate, detail.Balance, detail.Payments, detail.PackageName, detail.LastPaymentDate, detail.Notes, detail.PackageNotes, (double)0.00 );
					freezeDate = DateTime.MinValue;
					unfreezeDate = DateTime.MinValue;
					freezeRange = new TimeSpan((long)0);
					_freezeTable = new SortedList();
					
				}
				
				// store this session for the next iteration
				prevDetail = detail;
												
			}				
			
			if( prevDetail != null )
			{
				
				// Last item
				if( log.IsDebugEnabled )
				{
					log.Debug( string.Format( "Last session write, complete processing of last: {0}", prevDetail ) );
				}					
								
				//##############
				if( _freezeTable.Count > 0 )
				{					
					foreach ( DictionaryEntry myDE in expectedBalanceTable )
					{																				
						log.Debug( string.Format( "BEFORE FREEZE: \t{0}:\t{1:c}", (DateTime)myDE.Key, (double)myDE.Value ) );														
					}		
				}
				
				// Apply freeze Table
				foreach ( DictionaryEntry ft in _freezeTable )
				{						
					// load temp table
					SortedList expectedBalanceTableTemp = new SortedList();
					
					log.Debug( string.Format( "FREEZE TABLE: \t{0}:\t{1}", (DateTime)ft.Key, (DateTime)ft.Value ) );
					TimeSpan currentFreezeRange = ((DateTime)ft.Value).Subtract( (DateTime)ft.Key );
					log.Debug( string.Format( "currentFreezeRange: {0}", currentFreezeRange ) );
																																
					foreach ( DictionaryEntry myDE in expectedBalanceTable )
					{												
						if( ((DateTime)myDE.Key).CompareTo( (DateTime)ft.Key ) >= 0 )
						{								
							// if the freeze date is greater than or equal to the payment date, 
							// then increase dates forward by currentFreezeRange
							expectedBalanceTableTemp.Add( ((DateTime)myDE.Key).Add( currentFreezeRange ), (double)myDE.Value );
						}
						else
						{
							// Save current record - no changes necessary
							expectedBalanceTableTemp.Add( (DateTime)myDE.Key, (double)myDE.Value );
						}
					}
					
					// Reset to permanent table
					expectedBalanceTable = expectedBalanceTableTemp;
				}							
							
				if( _freezeTable.Count > 0 )
				{						
					foreach ( DictionaryEntry myDE in expectedBalanceTable )
					{																				
						log.Debug( string.Format( "AFTER FREEZE: \t{0}:\t{1:c}", (DateTime)myDE.Key, (double)myDE.Value ) );														
					}								
				}							
				
				//##############				
				
				ArrayList _payments = new ArrayList();
				
				// Adjust payment count
				int numberPayments = expectedBalanceTable.Count;
				if( !"M".Equals( prevDetail.PayMethod ) )
				{
					numberPayments = 1;
					log.Debug( string.Format( "numberPayments: [{0}]", numberPayments ) );
					
					// Sum up all payments from table
					double rt = (double)0;
					foreach ( DictionaryEntry myDE in expectedBalanceTable )
					{															
						rt = rt + (double)myDE.Value;
					}							
					
					PaymentVO vo = new PaymentVO();
					vo.AccessCode = prevDetail.AccessCode;
					vo.BillingType = "S";
					vo.PaymentGroup = string.Format("{0}", prevDetail.TrackingNumber ); //analogous to membership id / session id
					vo.PaymentNumber = 1;														
					
					vo.PaymentDueDate = prevDetail.StartDate;										
					
					vo.RateAmount = (decimal)rt;			
					double iniamt = (double)prevDetail.InitFee;
                    vo.InitAmount = (decimal)iniamt;
                    double ini2amt = (double)prevDetail.InitFee2;
                    vo.Init2Amount = (decimal)ini2amt;
                    double ini3amt = (double)prevDetail.InitFee3;
                    vo.Init3Amount = (decimal)ini3amt;
                    double ini4amt = (double)prevDetail.InitFee4;
                    vo.Init4Amount = (decimal)ini4amt;
                    double ini5amt = (double)prevDetail.InitFee5;
                    vo.Init5Amount = (decimal)ini5amt;

					vo.AccrualAmount = (decimal)0;
                    vo.ExtraChargeAmount = (decimal)0;
                    vo.ExtraChargeAmount2 = (decimal)0;
                    vo.ExtraChargeAmount3 = (decimal)0;
                    vo.ExtraChargeAmount4 = (decimal)0;
                    vo.ExtraChargeAmount5 = (decimal)0;
					vo.CreditAmt = (decimal)0;
					vo.PaidAmount = (decimal)0;		
					
					_payments.Add( vo );					
				}
				else
				{
					numberPayments = numberPayments - ( firstMonthFactor - 1 );
					if( numberPayments <= 0 )
					{
						numberPayments = 1;
					}						
					log.Debug( string.Format( "numberPayments: [{0}]", numberPayments ) );
					
					int i = 0;
					foreach ( DictionaryEntry myDE in expectedBalanceTable )
					{	
						i++;
						if( i <= numberPayments )
						{
							PaymentVO vo = new PaymentVO();
							vo.AccessCode = prevDetail.AccessCode;
							vo.BillingType = "S";
							vo.PaymentGroup = string.Format("{0}", prevDetail.TrackingNumber ); //analogous to membership id / session id
							vo.PaymentNumber = i;														
							
							vo.PaymentDueDate = (DateTime)myDE.Key;
							
							double rt = (double)myDE.Value;
							
							vo.RateAmount = (decimal)rt;
							vo.InitAmount = (decimal)0;					
							if( i == 1 )
							{
								double iniamt = (double)prevDetail.InitFee;
								vo.InitAmount = (decimal)iniamt;
                            }
                            vo.Init2Amount = (decimal)0;
                            if (i == 1)
                            {
                                double ini2amt = (double)prevDetail.InitFee2;
                                vo.Init2Amount = (decimal)ini2amt;
                            }
                            vo.Init3Amount = (decimal)0;
                            if (i == 1)
                            {
                                double ini3amt = (double)prevDetail.InitFee3;
                                vo.Init3Amount = (decimal)ini3amt;
                            }
                            vo.Init4Amount = (decimal)0;
                            if (i == 1)
                            {
                                double ini4amt = (double)prevDetail.InitFee4;
                                vo.Init4Amount = (decimal)ini4amt;
                            }
                            vo.Init5Amount = (decimal)0;
                            if (i == 1)
                            {
                                double ini5amt = (double)prevDetail.InitFee5;
                                vo.Init5Amount = (decimal)ini5amt;
                            }
							vo.AccrualAmount = (decimal)0;
                            vo.ExtraChargeAmount = (decimal)0;
                            vo.ExtraChargeAmount2 = (decimal)0;
                            vo.ExtraChargeAmount3 = (decimal)0;
                            vo.ExtraChargeAmount4 = (decimal)0;
                            vo.ExtraChargeAmount5 = (decimal)0;
							vo.CreditAmt = (decimal)0;
							vo.PaidAmount = (decimal)0;		
							
							_payments.Add( vo );
						}
					}																		
				}
				
				decimal _servicePrice = (decimal)0;
				foreach( PaymentVO pv in _payments )
				{
					log.Debug( pv );
                    _servicePrice = _servicePrice + pv.RateAmount + pv.InitAmount + pv.Init2Amount + pv.Init3Amount + pv.Init4Amount + pv.Init5Amount + pv.ExtraChargeAmount + pv.ExtraChargeAmount2 + pv.ExtraChargeAmount3 + pv.ExtraChargeAmount4 + pv.ExtraChargeAmount5 + pv.AccrualAmount;
				}
				
				log.Debug( string.Format( "_servicePrice [{0}]", _servicePrice ) );						
				
		        UserDAO udao = new UserDAO();
		        UserBaseVO uvo = udao.retrieveUserVOByAccessCode( prevDetail.AccessCode );						
								        				        
				SqlConnection connection2 = new SqlConnection( ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] ) );				
				connection2.Open();
				SqlTransaction transaction2 = connection2.BeginTransaction();			
				int returnValue = -1;
				try
				{																
					
					TrainingPackageDAO tpdao = new TrainingPackageDAO();
					returnValue = tpdao.insertConvertedScheduledPayments(_payments, prevDetail.AccessCode, string.Format("{0}", prevDetail.TrackingNumber ), (decimal)prevDetail.Payments, connection2, transaction2, user);
							
					if( returnValue < 0 )
					{
						Auditor auditor = new Auditor( user );
						AuditVO avo = new AuditVO();
						avo.Function = AuditFunctions.CONVERSION.ToString();
						avo.SubFunction = AuditSubFunctionsConversion.SERVICES_FAILURE.ToString();
						avo.PreImage = string.Format("Service [{0}-{1}] for {2} failed conversion: schedPaymentsCount[{3}], amount[{4:C}], totalPaid[{5:C}]", prevDetail.TrackingNumber, prevDetail.PackageName, prevDetail.DisplayName, numberPayments, _servicePrice, prevDetail.Payments );
						avo.ActionOnUser = uvo.DisplayName;
						avo.ActionOnUserGroup = uvo.UserGroup;								
						auditor.audit( avo );
						throw new Exception();
					}
					else
					{
						Auditor auditor = new Auditor( user );
						AuditVO avo = new AuditVO();
						avo.Function = AuditFunctions.CONVERSION.ToString();
						avo.SubFunction = AuditSubFunctionsConversion.SERVICES_SUCCESS.ToString();
						avo.PreImage = string.Format("Service [{0}-{1}] for {2} converted to payment table: schedPaymentsCount[{3}], amount[{4:C}], totalPaid[{5:C}]", prevDetail.TrackingNumber, prevDetail.PackageName, prevDetail.DisplayName, numberPayments, _servicePrice, prevDetail.Payments );
						avo.ActionOnUser = uvo.DisplayName;
						avo.ActionOnUserGroup = uvo.UserGroup;
						
						auditor.audit( avo, connection2, transaction2 );
					}							
					
					// Everything good - commit this transaction
					transaction2.Commit();
					
					// Close this connection
					connection2.Close();					
				}		
				catch( Exception tranE )
				{
					transaction2.Rollback();
					string msg = string.Format( "Adding scheduled service payments failed: [{0}-{1}] for {2}", prevDetail.TrackingNumber, prevDetail.PackageName, prevDetail.DisplayName );
					log.Error( msg, tranE );
					throw new Exception( msg, tranE );
					// keep processing.
				}		
				finally
				{				
					if ( connection2 != null )			
					{
						connection2.Close();
						connection2 = null;
					}			
				}						
				
				// update summaryVO with expected balance and add it to the array list
				// first sum all of the amounts in the table with pay dates less than 
				// or equal to today
				foreach ( DictionaryEntry myDE in expectedBalanceTable )
				{																				
					if( todaysDate.CompareTo( ((DateTime)myDE.Key).Add( freezeRange ) ) >= 0 )
					{					
						if( expectedBalanceTable.IndexOfKey( myDE.Key ) <= ( expectedBalanceTable.Count - firstMonthFactor ) )
						{
							summaryVO.TotalAmountDue = summaryVO.TotalAmountDue + (double)myDE.Value;
						}													
					}
				}			
				
				if( !"M".Equals( prevDetail.PayMethod ) )
				{
					summaryVO.TotalAmountDue = prevDetail.TotalAmount - summaryVO.Payments;
				}
				else
				{
                    summaryVO.TotalAmountDue = summaryVO.TotalAmountDue - summaryVO.Payments + prevDetail.InitFee + prevDetail.InitFee2 + prevDetail.InitFee3 + prevDetail.InitFee4 + prevDetail.InitFee5;
				}
							
				if( log.IsDebugEnabled )
				{
					log.Debug( string.Format( "Total Amount Due is [{0}]", summaryVO.TotalAmountDue ) ) ;
				}				
				
				if( summaryVO.TotalAmountDue.CompareTo( (double)0.01 ) >= 0 )
				{											
					summaryList.Add( summaryVO );
				}
			}
									
			return summaryList;		
		}			    
	    
		public ArrayList retrievePrePaidOpenBalanceList( OpenBalancePrePaidListSearchVO searchCriteria  )
		{			
			
			if( searchCriteria == null )
			{
				string msg = "OpenBalancePrePaidListSearchVO is null.";
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("OpenBalancePrePaidListSearchVO is [{0}]", searchCriteria) );
			}
			
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;
			SqlParameter idParam = null;
			ArrayList list = new ArrayList();
			
			// 1. Get the connection string.
			string connectionParam = ShapeConnection.retrieveConnection( ConfigurationManager.AppSettings["SQLDBConnString"] );
			UserStatusEnumDatabaseMapper userStatusDbMapper = UserStatusEnumDatabaseMapper.getInstance();

			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();

				// 2. Query to retrieve results.
				command = new SqlCommand( "sp_get_open_balances_prepaid", connection );
				command.CommandType = CommandType.StoredProcedure;				
				
				idParam = command.Parameters.Add("@access_code", SqlDbType.Int );
				idParam.Value = searchCriteria.AccessCode;				
				if( searchCriteria.AccessCode == 0 )
				{
					idParam.Value = DBNull.Value;
				}				
				
				idParam = command.Parameters.Add("@uid", SqlDbType.NVarChar, 40 );
				idParam.Value = searchCriteria.Uid;				
				if( searchCriteria.Uid == null || string.Empty.Equals( searchCriteria.Uid.Trim() ) )
				{
					idParam.Value = DBNull.Value;
				}
				
				idParam = command.Parameters.Add("@uid_operator", SqlDbType.NVarChar, 2 );
				idParam.Value = searchCriteria.UidOperator;	
				if( searchCriteria.UidOperator == null || string.Empty.Equals( searchCriteria.UidOperator.Trim() ) )
				{
					idParam.Value = DBNull.Value;
				}		
												
				idParam = command.Parameters.Add("@user_status", SqlDbType.NVarChar, 1 );
				if( searchCriteria.UserStatus == 0x0000 )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = userStatusDbMapper.mapEnumAsString( searchCriteria.UserStatus );
				}		
				
								
				if( log.IsDebugEnabled )
				{
				   for (int i = 0; i < command.Parameters.Count; i++) 
				   {
				   	log.Debug( string.Format( "{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value ) );
				   }
				}					
				
				// Retrieve data
				rs = command.ExecuteReader();							

				while( rs.Read() )
				{						
					int _accessCode = ( rs.IsDBNull( rs.GetOrdinal("access_code") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("access_code") ) );
					string _notes = ( rs.IsDBNull( rs.GetOrdinal("notes") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("notes") ) ).Trim();					
					string _uid = ( rs.IsDBNull( rs.GetOrdinal("uid") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("uid") ) ).Trim();
					string _userStatus = ( rs.IsDBNull( rs.GetOrdinal("active") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("active") ) ).Trim();					
					decimal _balance = ( rs.IsDBNull( rs.GetOrdinal("balance") ) )  ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("balance") ) );								
					string _email = ( rs.IsDBNull( rs.GetOrdinal("email") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("email") ) ).Trim();					
					string _firstName = ( rs.IsDBNull( rs.GetOrdinal("first_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("first_name") ) ).Trim();					
					string _lastName = ( rs.IsDBNull( rs.GetOrdinal("last_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("last_name") ) ).Trim();
					string _displayName = ( rs.IsDBNull( rs.GetOrdinal("display_name") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("display_name") ) ).Trim();
                    bool _doNotContact = (rs.IsDBNull(rs.GetOrdinal("do_not_contact"))) ? false : (rs.GetBoolean(rs.GetOrdinal("do_not_contact")));
                    string _smsGateway = (rs.IsDBNull(rs.GetOrdinal("smsGateway"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("smsGateway"))).Trim();
                    bool _smsActiveGateway = (rs.IsDBNull(rs.GetOrdinal("smsGatewayActive"))) ? false : (rs.GetBoolean(rs.GetOrdinal("smsGatewayActive")));
                    int _smsGatewayId = (rs.IsDBNull(rs.GetOrdinal("smsGatewayId"))) ? (int)0 : (rs.GetInt32(rs.GetOrdinal("smsGatewayId")));
                    string _cellPhone = (rs.IsDBNull(rs.GetOrdinal("cell_phone"))) ? string.Empty : (rs.GetString(rs.GetOrdinal("cell_phone"))).Trim();

              	    //Create new OpenBalancePrePaidVO object
					OpenBalancePrePaidVO vo = new OpenBalancePrePaidVO();
					vo.AccessCode = _accessCode;
					vo.Notes = _notes;
					vo.Uid = _uid;
					vo.UserStatus = (UserStatusEnum)userStatusDbMapper.mapStringAsEnum( _userStatus );
					vo.Balance = _balance;
					vo.Email = _email;
					vo.FirstName = _firstName;
					vo.LastName = _lastName;
					vo.DisplayName = _displayName;
                    vo.SmsGateway = _smsGateway;
                    vo.SmsGatewayActive = _smsActiveGateway;
                    vo.SmsGatewayId = _smsGatewayId;
                    vo.DoNotContact = _doNotContact;
                    vo.CellPhone = _cellPhone;
					
					// Add to ArrayList as a validation check
					list.Add( vo );
										
				}
								
				// Close reader.
				rs.Close();						
				
				connection.Close();

			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw e;
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}				
			
			return list;
		}

        public ArrayList retrieveCombinedOpenBalanceList(OpenBalanceCombinedListSearchVO searchCriteria, UserVO currentUser)
		{
			ArrayList results = new ArrayList();
			Hashtable list = new Hashtable();
			
			// membership open blanaces
			OpenBalanceMembershipListSearchVO sc_m = new OpenBalanceMembershipListSearchVO();
			sc_m.AccessCode = searchCriteria.AccessCode;
			sc_m.Cid = searchCriteria.Uid;
			sc_m.DelinquencyLevel = searchCriteria.DelinquencyLevel;
            sc_m.DaysPastDue = searchCriteria.DaysPastDue;
            sc_m.MembershipStatus = searchCriteria.MembershipStatus;
			sc_m.CidOperator = searchCriteria.UidOperator;
			sc_m.UserStatus = searchCriteria.UserStatus;
			sc_m.UserGroup = searchCriteria.UserGroup;
			sc_m.UserStatusActiveAndInactive = searchCriteria.UserStatusActiveAndInactive;
			
			results = this.retrieveMembershipOpenBalancesList( sc_m );
			if( results != null && results.Count > 0 )
			{
				IEnumerator ie = results.GetEnumerator();
				while( ie.MoveNext() )
				{
					MembershipOpenBalancesVO vo = (MembershipOpenBalancesVO)ie.Current;
					
					if( searchCriteria.HideNegative.Equals( "Y" ) && vo.AmountDue < 0)
					{
						continue;
					}
					
					if( searchCriteria.NonZeroOnly && vo.AmountDue == 0)
					{
						continue;
					}

					if (vo.IgnoreOpenBalance)
					{
						// donot count this open balance
						continue;
					}
					if (vo.AutoLockPastDueDays >= 0 && vo.AutoLockPastDueDays >= vo.DaysDelinquent)
                    {
                        // donot count this open balance
                        continue;
                    }

                    OpenBalanceCombinedVO current_obcvo = null;
					if( !list.ContainsKey( vo.AccessCode ) )
					{
						current_obcvo = new OpenBalanceCombinedVO();
						current_obcvo.AccessCode = vo.AccessCode;
						current_obcvo.AlertLevel = vo.AlertLevel;
						current_obcvo.DisplayName = vo.DisplayName;
						current_obcvo.Email = vo.UserEmail;
						current_obcvo.InvoicePreference = vo.InvoicePreference;
						current_obcvo.MailingLabelData = vo.MailingLabelData;
						current_obcvo.Notes = vo.Notes;
						current_obcvo.Uid = vo.UserId;
						current_obcvo.UserGroup = vo.UserGroup;
						current_obcvo.UserStatus = vo.UserStatus;
						current_obcvo.SmsGateway =  vo.SmsGateway;
                        current_obcvo.SmsGatewayActive = vo.SmsGatewayActive;
                        current_obcvo.SmsGatewayId = vo.SmsGatewayId;
						
                        list.Add( vo.AccessCode, current_obcvo );
					}
					
					current_obcvo = (OpenBalanceCombinedVO)( list[ vo.AccessCode ] );
					
					current_obcvo.MembershipAmountDue = vo.AmountDue;
					current_obcvo.MembershipDaysDelinquent = vo.DaysDelinquent;
					current_obcvo.MembershipDelinquencyLevel = vo.DelinquencyLevel;
					current_obcvo.MembershipDelinquentDate = vo.DelinquentDate;
					current_obcvo.IsMembershipEftPending = vo.IsEftPending;
				}
			}
			
			// service open balances
			OpenBalanceServiceListSearchVO sc_s = new OpenBalanceServiceListSearchVO();
			sc_s.AccessCode = searchCriteria.AccessCode;
			sc_s.DelinquencyLevel = searchCriteria.DelinquencyLevel;
            sc_s.DaysPastDue = searchCriteria.DaysPastDue;
            sc_s.HideNegative = searchCriteria.HideNegative;
			sc_s.Uid = searchCriteria.Uid;
			sc_s.UidOperator = searchCriteria.UidOperator;
			sc_s.UserGroup = searchCriteria.UserGroup;
			sc_s.UserStatus = searchCriteria.UserStatus;
			sc_s.UserStatusActiveAndInactive = searchCriteria.UserStatusActiveAndInactive;

            results = this.retrieveServiceOpenBalancesList(sc_s, currentUser);
			if( results != null && results.Count > 0 )
			{
				IEnumerator ie = results.GetEnumerator();
				while( ie.MoveNext() )
				{
					ServiceOpenBalancesVO vo = (ServiceOpenBalancesVO)ie.Current;
					
					if( searchCriteria.HideNegative.Equals( "Y" ) && vo.AmountDue < 0)
					{
						continue;
					}
					
					if( searchCriteria.NonZeroOnly && vo.AmountDue == 0)
					{
						continue;
					}
					
					OpenBalanceCombinedVO current_obcvo = null;
					if( !list.ContainsKey( vo.AccessCode ) )
					{
						current_obcvo = new OpenBalanceCombinedVO();
						current_obcvo.AccessCode = vo.AccessCode;
						current_obcvo.AlertLevel = vo.AlertLevel;
						current_obcvo.DisplayName = vo.DisplayName;
						current_obcvo.Email = vo.UserEmail;
						current_obcvo.InvoicePreference = vo.InvoicePreference;
						current_obcvo.MailingLabelData = vo.MailingLabelData;
						current_obcvo.Notes = vo.Notes;
						current_obcvo.Uid = vo.Uid;
						current_obcvo.UserGroup = vo.UserGroup;
						current_obcvo.UserStatus = vo.UserStatus;
                        current_obcvo.SmsGateway = vo.SmsGateway;
                        current_obcvo.SmsGatewayActive = vo.SmsGatewayActive;
                        current_obcvo.SmsGatewayId = vo.SmsGatewayId;
						
                        list.Add( vo.AccessCode, current_obcvo );
					}
					
					current_obcvo = (OpenBalanceCombinedVO)( list[ vo.AccessCode ] );
					
					// multiple service packages for a single user are listed separately and need to be combined
					current_obcvo.ServicesAmountDue += vo.AmountDue;
					current_obcvo.IsServicesEftPending |= vo.IsEftPending;
					
					if( vo.DaysDelinquent > current_obcvo.ServicesDaysDelinquent )
					{
						current_obcvo.ServicesDaysDelinquent = vo.DaysDelinquent;
						current_obcvo.ServicesDelinquencyLevel = vo.DelinquencyLevel;
						current_obcvo.ServicesDelinquentDate = vo.DelinquentDate;
					}
				}
			}
			
			// POS open balances
			OpenBalancePosListSearchVO sc_p = new OpenBalancePosListSearchVO();
			sc_p.AccessCode = searchCriteria.AccessCode;
			sc_p.DelinquencyLevel = searchCriteria.DelinquencyLevel;
            sc_p.DaysPastDue = searchCriteria.DaysPastDue;
            sc_p.HideNegative = searchCriteria.HideNegative;
			sc_p.Uid = searchCriteria.Uid;
			sc_p.UidOperator = searchCriteria.UidOperator;
			sc_p.UserGroup = searchCriteria.UserGroup;
			sc_p.UserStatus = searchCriteria.UserStatus;
			sc_p.UserStatusActiveAndInactive = searchCriteria.UserStatusActiveAndInactive;
			
			results = this.retrievePosOpenBalanceList( sc_p );
			if( results != null && results.Count > 0 )
			{
				IEnumerator ie = results.GetEnumerator();
				while( ie.MoveNext() )
				{
					OpenBalancePosVO vo = (OpenBalancePosVO)ie.Current;
					
					if( searchCriteria.HideNegative.Equals( "Y" ) && vo.AmountDue < 0)
					{
						continue;
					}
					
					if( searchCriteria.NonZeroOnly && vo.AmountDue == 0)
					{
						continue;
					}
					
					OpenBalanceCombinedVO current_obcvo = null;
					if( !list.ContainsKey( vo.AccessCode ) )
					{
						current_obcvo = new OpenBalanceCombinedVO();
						current_obcvo.AccessCode = vo.AccessCode;
						current_obcvo.AlertLevel = vo.AlertLevel;
						current_obcvo.DisplayName = vo.DisplayName;
						current_obcvo.Email = vo.Email;
						current_obcvo.InvoicePreference = vo.InvoicePreference;
						current_obcvo.MailingLabelData = vo.MailingLabelData;
						current_obcvo.Notes = vo.Notes;
						current_obcvo.Uid = vo.Uid;
						current_obcvo.UserGroup = vo.UserGroup;
						current_obcvo.UserStatus = vo.UserStatus;
                        current_obcvo.SmsGateway = vo.SmsGateway;
                        current_obcvo.SmsGatewayActive = vo.SmsGatewayActive;
                        current_obcvo.SmsGatewayId = vo.SmsGatewayId;
						
                        list.Add( vo.AccessCode, current_obcvo );
					}
					
					current_obcvo = (OpenBalanceCombinedVO)( list[ vo.AccessCode ] );
					
					// multiple transactions for a single user are listed separately and need to be combined
					current_obcvo.PosAmountDue += vo.AmountDue;
					current_obcvo.IsPosEftPending = vo.IsEftPending;
					if( vo.DaysDelinquent > current_obcvo.ServicesDaysDelinquent )
					{
						current_obcvo.PosDaysDelinquent = vo.DaysDelinquent;
						current_obcvo.PosDelinquencyLevel = vo.DelinquencyLevel;
						current_obcvo.PosDelinquentDate = vo.DelinquentDate;
					}
				}
			}
			
			// put everything into an arraylist
			ArrayList rv_list = new ArrayList();
			IDictionaryEnumerator ide = list.GetEnumerator();
			while( ide.MoveNext() )
			{
				OpenBalanceCombinedVO vo = (OpenBalanceCombinedVO)ide.Value;
				
				if( log.IsDebugEnabled )
				{
					log.Debug( vo );
				}
				
				rv_list.Add( vo );
			}
			
			return rv_list;
		}
			
		#region asynchronous MF reports
		
		public ArrayList retrieveMFMembershipMoneyTransactionsSummaryList( FacilityVO fac, MFMembershipMoneyTransactionsSummarySearchVO searchCriteria  )
		{			

			if( fac == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "FacilityVO [{0}] is null.", fac );
				log.Error( msg );
				throw new Exception( msg );
			}					
			
			if( searchCriteria == null )
			{
				// Should throw an exception and log this.
				string msg = string.Format( "MFMembershipMoneyTransactionsSummarySearchVO [{0}] is null.", searchCriteria );
				log.Error( msg );
				throw new Exception( msg );
			}		
			
			if( log.IsDebugEnabled )
			{
				log.Debug( string.Format("MFMembershipMoneyTransactionsSummarySearchVO is [{0}], Facility is [{1}]", searchCriteria, fac.ShortName ) );
			}
														
			ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();
			
			string connectionParam = ShapeConnection.retrieveConnection( fac.ConnectionString );
			
			SqlConnection connection = null;
			SqlCommand command = null;
			SqlDataReader rs = null;								
			ArrayList list = new ArrayList();

            string sqltimeoutstr = ShapeConfiguration.AppSettings("SQLTimeOut");
            int sqltimeout = 180;
            if (!Int32.TryParse(sqltimeoutstr, out sqltimeout))
            {
                sqltimeout = 180;
            } 
			try
			{
				// Create and open connection.
				connection = new SqlConnection( connectionParam );
				connection.Open();				
				
				// 2. Query to retrieve the user.
				command = new SqlCommand( "sp_get_membership_money_summary", connection );
                command.CommandTimeout = sqltimeout;
				command.CommandType = CommandType.StoredProcedure;							
									
				SqlParameter idParam = null;
				
				idParam = command.Parameters.Add("@minRecordDate", SqlDbType.DateTime );
				if( searchCriteria.EffDate == null || searchCriteria.EffDate.Item == null )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = (DateTime)(searchCriteria.EffDate.Item);
				}			
				
				idParam = command.Parameters.Add("@maxRecordDate", SqlDbType.DateTime );
				if( searchCriteria.EffDate == null || searchCriteria.EffDate.Item2 == null )
				{
					idParam.Value = DBNull.Value;
				}				
				else
				{
					idParam.Value = (DateTime)(searchCriteria.EffDate.Item2);
				}

                idParam = command.Parameters.Add("@uniqueGroupID", SqlDbType.NVarChar, 10);
                idParam.Value = DBNull.Value;
                if (!String.IsNullOrEmpty(searchCriteria.UniqueGroupID))
                {
                    idParam.Value = searchCriteria.UniqueGroupID;
                }	
													
                if (log.IsDebugEnabled)
                {
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {                    	
                    	if( log.IsDebugEnabled )
                    	{
                        	log.Debug(string.Format("{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value));
                    	}
                    }
                }							
								
				// Retrieve data
				rs = command.ExecuteReader();			
					
				decimal _ca_amount = (decimal)0.00;
				decimal _cc_amount = (decimal)0.00;
				decimal _ec_amount = (decimal)0.00;
				decimal _ba_amount = (decimal)0.00;
				int _ca_count = 0;
				int _cc_count = 0;
				int _ec_count = 0;
				int _ba_count = 0;
										
				while( rs.Read() )
				{
					decimal _amount = ( rs.IsDBNull( rs.GetOrdinal("amount") ) ) ? (decimal)0.00 : ( rs.GetDecimal( rs.GetOrdinal("amount") ) );
					int _itemCount = ( rs.IsDBNull( rs.GetOrdinal("itemCount") ) ) ? (int)0 : ( rs.GetInt32( rs.GetOrdinal("itemCount") ) );
					string _payType = ( rs.IsDBNull( rs.GetOrdinal("payType") ) ) ? string.Empty : ( rs.GetString( rs.GetOrdinal("payType") ) ).Trim();						
					
					switch( _payType )
					{
						case "CC":
							_cc_amount = _amount;
							_cc_count = _itemCount;
							break;
							
						case "CA":
							_ca_amount = _amount;
							_ca_count = _itemCount;
							break;
							
						case "BA":
							_ba_amount = _amount;
							_ba_count = _itemCount;
							break;
							
						case "EC":
							_ec_amount = _amount;
							_ec_count = _itemCount;
							break;								
					}
				}
				
				MFMembershipMoneyTransactionsSummaryVO vo = new MFMembershipMoneyTransactionsSummaryVO( _ca_amount, _cc_amount, _ec_amount, _ba_amount, _ca_count, _cc_count, _ec_count, _ba_count );
				vo.Facility = fac;
								
				// Close reader.
				rs.Close();												
				
				connection.Close();
				
				list.Add( vo );

			}
			catch( Exception e )
			{
				log.Error( e.ToString() );
				throw new Exception();
			}
			finally
			{
			
				if ( connection != null )			
				{
					connection.Close();
					connection = null;
				}			
			}												
			
			return list;		
		}

        #endregion


        public ArrayList retrieveSalesTax(SalesTaxSearchVO searchCriteria)
        {
            if (searchCriteria == null)
            {
                // Should throw an exception and log this.
                string msg = string.Format("retrieveSalesTax [{0}] is null.", searchCriteria);
                log.Error(msg);
                throw new Exception(msg);
            }
            if (log.IsDebugEnabled)
            {
                log.Debug(string.Format("retrieveSalesTax is [{0}]", searchCriteria));
            }

            ComparisonOperatorEnumDatabaseMapper comparisonOperatorDbMapper = ComparisonOperatorEnumDatabaseMapper.getInstance();

            string connectionParam = ShapeConnection.retrieveConnection(ConfigurationManager.AppSettings["SQLDBConnString"]);

            SqlConnection connection = null;
            SqlCommand command = null;
            SqlDataReader rs = null;
            ArrayList list = new ArrayList();

            string sqltimeoutstr = ShapeConfiguration.AppSettings("SQLTimeOut");
            int sqltimeout = 180;
            if (!Int32.TryParse(sqltimeoutstr, out sqltimeout))
            {
                sqltimeout = 180;
            }
            try
            {
                // Create and open connection.
                connection = new SqlConnection(connectionParam);
                connection.Open();

                // 2. Query to retrieve the user.
                command = new SqlCommand("sp_get_salestax", connection);
                command.CommandTimeout = sqltimeout;
                command.CommandType = CommandType.StoredProcedure;

                SqlParameter idParam = null;

                idParam = command.Parameters.Add("@salesDate", SqlDbType.DateTime);
                if (searchCriteria.SalesDate == null || searchCriteria.SalesDate.Item == null)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = (DateTime)(searchCriteria.SalesDate.Item);
                }

                idParam = command.Parameters.Add("@salesDateOperator", SqlDbType.NVarChar, 2);
                if (searchCriteria.SalesDate == null || searchCriteria.SalesDate.Operator == 0x0000)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = comparisonOperatorDbMapper.mapEnumAsString(searchCriteria.SalesDate.Operator);
                }

                idParam = command.Parameters.Add("@salesDate2", SqlDbType.DateTime);
                if (searchCriteria.SalesDate == null || searchCriteria.SalesDate.Item2 == null)
                {
                    idParam.Value = DBNull.Value;
                }
                else
                {
                    idParam.Value = (DateTime)(searchCriteria.SalesDate.Item2);
                }

                if (log.IsDebugEnabled)
                {
                    for (int i = 0; i < command.Parameters.Count; i++)
                    {
                        log.Debug(string.Format("{0}={1};", command.Parameters[i].ParameterName, command.Parameters[i].Value));
                    }
                }

                // Retrieve data
                rs = command.ExecuteReader();

                while (rs.Read())
                {
                    decimal _totalSales = (rs.IsDBNull(rs.GetOrdinal("totalSales"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("totalSales")));
                    decimal _salesTax = (rs.IsDBNull(rs.GetOrdinal("salesTax"))) ? (decimal)0.00 : (rs.GetDecimal(rs.GetOrdinal("salesTax")));

                    SalesTaxVO vo = new SalesTaxVO();
					vo.TotalSales = _totalSales;
					vo.SalesTax = _salesTax;
                    // Add to ArrayList as a validation check
                    list.Add(vo);


                }

                // Close reader.
                rs.Close();
                connection.Close();
            }
            catch (Exception e)
            {
                log.Error(e.ToString());
                throw new Exception();
            }
            finally
            {
                if (connection != null)
                {
                    connection.Close();
                    connection = null;
                }
            }

            return list;
        }

    }
}

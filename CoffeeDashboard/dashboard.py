import streamlit as st
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import altair as alt
from datetime import datetime, date
import pymongo
from pymongo import MongoClient
# Import configuration
from config import config, validate_config  

@st.cache_resource
def init_connection():
    """Initialize MongoDB connection using environment variables"""
    try:
        # Get configuration from environment variables
        mongo_uri = config.get_mongo_uri()
        db_name = config.get_mongo_database()
        collection_name = config.get_mongo_collection()
        connection_timeout = config.get_mongo_connection_timeout()
        server_selection_timeout = config.get_mongo_server_selection_timeout()
        
        # Create MongoDB client with timeout settings
        client = MongoClient(
            mongo_uri,
            connectTimeoutMS=connection_timeout,
            serverSelectionTimeoutMS=server_selection_timeout
        )
        
        # Test connection 
        client.admin.command('ping')
        
        return client, db_name, collection_name
    except Exception as e:
        st.error(f"Không thể kết nối MongoDB: {str(e)}")
        if config.is_debug_mode():
            st.exception(e)
        return None, None, None

# ----- Load Data từ MongoDB được tối ưu hóa với số nguyên -----
@st.cache_data(ttl=config.get_cache_ttl())
def load_data_optimized():
    """Load dữ liệu được tối ưu hóa với pipeline MongoDB và đảm bảo tất cả số là số nguyên"""
    try:
        # Get configuration
        mongo_uri = config.get_mongo_uri()
        db_name = config.get_mongo_database()
        collection_name = config.get_mongo_collection()
        connection_timeout = config.get_mongo_connection_timeout()
        server_selection_timeout = config.get_mongo_server_selection_timeout()
        
        # Create new MongoDB client for each call
        client = MongoClient(
            mongo_uri,
            connectTimeoutMS=connection_timeout,
            serverSelectionTimeoutMS=server_selection_timeout
        )
        
        # Test connection
        client.admin.command('ping')
        
        db = client[db_name]
        collection = db[collection_name]
        
        # Pipeline tối ưu - chỉ lấy các trường cần thiết
        pipeline = [
            {
                "$match": {
                    "price": {"$gt": 0, "$lt": 1000000000}
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "name": 1,
                    "category": 1,
                    "price": {"$round": ["$price", 0]},  # Làm tròn giá thành số nguyên
                    "promotion": 1,
                    "stock_history": {
                        "$slice": [{"$ifNull": ["$stock_history", []]}, 50]  # Giới hạn lịch sử tồn kho
                    }
                }
            }
        ]
        
        cursor = collection.aggregate(pipeline, allowDiskUse=True, batchSize=500)
        
        all_data = []
        all_dates = set()
        
        for product in cursor:
            try:
                # Xử lý giá - đảm bảo là số nguyên
                price = int(round(float(product.get('price', 1000))))
                price = max(1000, price)  # Giá tối thiểu 1000 VND
                
                # Xử lý stock_history và tính toán tổng
                stock_history = product.get('stock_history', [])
                total_sold = 0
                total_stock_increased = 0
                
                for entry in stock_history:
                    if isinstance(entry, dict):
                        # Xử lý ngày
                        if 'date' in entry:
                            try:
                                entry_date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                                all_dates.add(entry_date)
                            except:
                                continue
                        
                        # Xử lý số lượng - đảm bảo là số nguyên
                        stock_decreased = entry.get('stock_decreased', 0)
                        stock_increased = entry.get('stock_increased', 0)
                        
                        # Chuyển đổi số khoa học (nếu có) và làm tròn
                        if isinstance(stock_decreased, str) and 'e' in stock_decreased.lower():
                            stock_decreased = float(stock_decreased)
                        total_sold += int(round(float(stock_decreased)))
                        
                        if isinstance(stock_increased, str) and 'e' in stock_increased.lower():
                            stock_increased = float(stock_increased)
                        total_stock_increased += int(round(float(stock_increased)))
                
                # Tính toán doanh thu - đảm bảo là số nguyên
                revenue = int(price * total_sold)
                stock_revenue = int(price * total_stock_increased)
                
                all_data.append({
                    'id': str(product.get('_id', '')),
                    'name': product.get('name', ''),
                    'category': product.get('category', ''),
                    'price': price,
                    'promotion': product.get('promotion', ''),
                    'total_sold': total_sold,
                    'revenue': revenue,
                    'total_stock_increased': total_stock_increased,
                    'stock_revenue': stock_revenue,
                    'stock_history': stock_history,
                    'source_file': 'MongoDB'
                })
                
            except Exception as e:
                if config.is_debug_mode():
                    st.error(f"Error processing product: {e}")
                continue
        
        df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
        
        if not all_dates:
            min_date = date(2025, 3, 5)
            max_date = date(2025, 5, 25)
        else:
            min_date = min(all_dates)
            max_date = max(all_dates)
        
        client.close()
        
        return df, min_date, max_date
        
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu từ MongoDB: {str(e)}")
        if config.is_debug_mode():
            st.exception(e)
        return pd.DataFrame(), date(2025, 3, 5), date(2025, 5, 25)

# ----- Hàm phân khúc sản phẩm theo giá -----
def apply_clustering_improved(df):
    """Áp dụng phân khúc sản phẩm theo giá cải tiến"""
    if df.empty:
        return df
    
    # Tạo copy để không thay đổi dataframe gốc
    df = df.copy()
    
    # Tính toán các cột cần thiết
    df['quantity_sold'] = df['total_sold'].astype(int)
    df['stock_remaining'] = df['total_stock_increased'].astype(int)
    
    # Phân khúc theo giá
    try:
        if len(df) > 1 and df['price'].nunique() > 1:
            # Sử dụng quantile để phân khúc
            price_25th = df['price'].quantile(0.33)
            price_75th = df['price'].quantile(0.67)
            
            def categorize_price(price):
                if pd.isna(price):
                    return 'Không xác định'
                elif price <= price_25th:
                    return 'Thấp'
                elif price <= price_75th:
                    return 'Trung bình'
                else:
                    return 'Cao'
            
            df['segment'] = df['price'].apply(categorize_price)
        else:
            df['segment'] = 'Trung bình'
            
    except Exception as e:
        st.warning(f"Lỗi khi phân khúc dữ liệu: {e}")
        df['segment'] = 'Trung bình'
    
    return df

# ----- Hàm lọc theo ngày với số nguyên -----
def filter_by_date_range_optimized(df, start_date, end_date):
    """Lọc dữ liệu theo khoảng thời gian với số nguyên"""
    if df.empty:
        return df
    
    try:
        filtered_df = df.copy()
        
        def filter_stock_history(stock_history):
            if not isinstance(stock_history, list):
                return []
            
            filtered_history = []
            for entry in stock_history:
                if isinstance(entry, dict) and 'date' in entry:
                    try:
                        entry_date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                        if start_date <= entry_date <= end_date:
                            # Xử lý số lượng - đảm bảo là số nguyên
                            entry_copy = entry.copy()
                            for key in ['stock_decreased', 'stock_increased']:
                                if key in entry_copy:
                                    val = entry_copy[key]
                                    if isinstance(val, str) and 'e' in val.lower():
                                        val = float(val)
                                    entry_copy[key] = int(round(float(val)))
                            filtered_history.append(entry_copy)
                    except:
                        continue
            return filtered_history
        
        filtered_df['stock_history'] = filtered_df['stock_history'].apply(filter_stock_history)
        
        def recalculate_metrics(row):
            stock_history = row['stock_history']
            total_sold = 0
            total_stock_increased = 0
            
            for entry in stock_history:
                if isinstance(entry, dict):
                    total_sold += max(0, int(round(float(entry.get('stock_decreased', 0)))))
                    total_stock_increased += max(0, int(round(float(entry.get('stock_increased', 0)))))
            
            return pd.Series({
                'quantity_sold': total_sold,
                'stock_remaining': total_stock_increased,
                'revenue': int(row['price'] * total_sold),
                'stock_revenue': int(row['price'] * total_stock_increased)
            })
        
        metrics = filtered_df.apply(recalculate_metrics, axis=1)
        filtered_df['quantity_sold'] = metrics['quantity_sold']
        filtered_df['stock_remaining'] = metrics['stock_remaining']  
        filtered_df['revenue'] = metrics['revenue']
        filtered_df['stock_revenue'] = metrics['stock_revenue']
        
        return filtered_df
        
    except Exception as e:
        st.warning(f"Lỗi khi lọc dữ liệu theo ngày: {e}")
        return df

# ----- Giao diện chính -----
st.set_page_config(
    layout="wide", 
    page_title=config.get_app_title(),
    page_icon="📊"
)

# Display environment info in debug mode
if config.is_debug_mode():
    st.sidebar.info(f"🔧 Environment: {config.get_environment()}")
    st.sidebar.info(f"🗃️ Database: {config.get_mongo_database()}")
    st.sidebar.info(f"📦 Collection: {config.get_mongo_collection()}")

# Load dữ liệu
try:
    df, min_date, max_date = load_data_optimized()
    if df.empty:
        st.error("""
        Không có dữ liệu từ MongoDB. Vui lòng kiểm tra:
        1. Kết nối MongoDB
        2. Tên database và collection
        3. Dữ liệu trong collection có đúng định dạng không?
        """)
        st.stop()
except Exception as e:
    st.error(f"Lỗi nghiêm trọng khi tải dữ liệu: {str(e)}")
    if config.is_debug_mode():
        st.exception(e)
    st.stop()

# Xử lý dữ liệu
df = apply_clustering_improved(df)

# ----- Sidebar Filters -----
st.sidebar.header("Bộ lọc")
selected_category = st.sidebar.selectbox("Chọn danh mục", ['Tất cả'] + sorted(df['category'].unique()))
filtered_df = df if selected_category == 'Tất cả' else df[df['category'] == selected_category]

segment_options = ['Tất cả'] + sorted(filtered_df['segment'].dropna().unique())
selected_segment = st.sidebar.selectbox("Phân khúc", segment_options)
if selected_segment != 'Tất cả':
    filtered_df = filtered_df[filtered_df['segment'] == selected_segment]

product_options = ['Tất cả'] + sorted(filtered_df['name'].unique())
selected_product = st.sidebar.selectbox("Sản phẩm", product_options)
if selected_product != 'Tất cả':
    filtered_df = filtered_df[filtered_df['name'] == selected_product]

display_mode = st.sidebar.selectbox("Chế độ hiển thị", ["Bán hàng", "Tồn kho"])

default_start = max(min_date, date(2025, 3, 5))
default_end = min(max_date, date(2025, 5, 18))

start_date = st.sidebar.date_input("Ngày bắt đầu", value=default_start, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("Ngày kết thúc", value=default_end, min_value=min_date, max_value=max_date)

filtered_df = filter_by_date_range_optimized(filtered_df, start_date, end_date)

# ----- Main Dashboard -----
st.title("📊 Dashboard Phân Khúc Doanh Thu và Tồn Kho - KingFoodMart")

st.markdown("""
<style>
div[data-testid="stHorizontalBlock"] > div { gap: 20px !important; }
div[data-testid="stMetric"] { min-width: 200px !important; padding: 15px !important; background-color: #f8f9fa; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); flex: 1; }
[data-testid="stMetricValue"] > div { font-size: 22px !important; font-weight: bold; color: #2c3e50; white-space: normal !important; overflow: visible !important; text-overflow: unset !important; line-height: 1.3; height: auto !important; }
[data-testid="stMetricLabel"] > div { font-size: 16px !important; font-weight: 600; color: #7f8c8d; margin-bottom: 10px !important; white-space: normal !important; overflow: visible !important; text-overflow: unset !important; height: auto !important; }
.stDataFrame { font-size: 14px; }
.vega-embed { padding-bottom: 20px !important; }
@media (max-width: 768px) { div[data-testid="stHorizontalBlock"] { flex-direction: column !important; } div[data-testid="stMetric"] { min-width: 100% !important; margin-bottom: 15px !important; } }
</style>
""", unsafe_allow_html=True)

# ----- KPI Metrics -----
st.subheader("📈Tổng Quan")
col1, col2, col3, col4 = st.columns(4)

def format_number(num):
    """Định dạng số nguyên với dấu chấm phân cách"""
    if pd.isna(num):
        return "0"
    return f"{int(num):,.0f}".replace(",", ".")

if not filtered_df.empty:
    total_revenue = int(filtered_df['revenue'].sum())
    total_stock_revenue = int(filtered_df['stock_revenue'].sum())
    total_quantity = int(filtered_df['quantity_sold'].sum())
    total_stock = int(filtered_df['stock_remaining'].sum())
    
    # Tính giá trung bình chỉ từ những sản phẩm có doanh thu > 0
    if display_mode == "Bán hàng":
        products_with_sales = filtered_df[filtered_df['quantity_sold'] > 0]
        avg_price = int(products_with_sales['price'].mean()) if len(products_with_sales) > 0 else 0
    else:
        products_with_stock = filtered_df[filtered_df['stock_revenue'] > 0]
        avg_price = int(products_with_stock['price'].mean()) if len(products_with_stock) > 0 else 0
else:
    total_revenue = total_stock_revenue = total_quantity = total_stock = avg_price = 0

with col1:
    if display_mode == "Bán hàng":
        st.metric("Tổng Doanh Thu", f"{format_number(total_revenue)} VND")
    else:
        st.metric("Tổng Doanh Thu Tồn Kho", f"{format_number(total_stock_revenue)} VND")

with col2:
    if display_mode == "Bán hàng":
        st.metric("Tổng Số Lượng Bán", format_number(total_quantity))
    else:
        st.metric("Tổng Tồn Kho", format_number(total_stock))

with col3:
    st.metric("Giá Trung Bình", f"{format_number(avg_price)} VND")

with col4:
    if display_mode == "Bán hàng":
        top_product = (
            filtered_df.loc[filtered_df['quantity_sold'].idxmax()]['name']
            if not filtered_df.empty and filtered_df['quantity_sold'].sum() > 0 else "N/A"
        )
        st.markdown(f"""
        <div class="custom-metric" style='background-color: #f8f9fa; padding: 15px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);'>
            <div style='font-size: 16px; font-weight: 600; color: #7f8c8d; margin-bottom: 10px;'>Sản Phẩm Bán Chạy</div>
            <div class="product-name">{top_product}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        top_stock_product = (
            filtered_df.loc[filtered_df['stock_remaining'].idxmax()]['name']
            if not filtered_df.empty and filtered_df['stock_remaining'].sum() > 0 else "N/A"
        )
        st.markdown(f"""
        <div class="custom-metric" style='background-color: #f8f9fa; padding: 15px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);'>
            <div style='font-size: 16px; font-weight: 600; color: #7f8c8d; margin-bottom: 10px;'>Sản Phẩm Tồn Kho Nhiều Nhất</div>
            <div class="product-name">{top_stock_product}</div>
        </div>
        """, unsafe_allow_html=True)

# ----- Top Sản Phẩm -----
st.subheader("🏆 Top Sản Phẩm")
if not filtered_df.empty:
    if display_mode == "Bán hàng":
        try:
            filtered_df['quantity_sold'] = filtered_df['quantity_sold'].astype(int)
        except Exception as e:
            st.error(f"Lỗi khi chuyển đổi quantity_sold: {str(e)}")
            st.stop()
        
        products_with_sales = filtered_df[filtered_df['quantity_sold'] > 0].copy()
        
        if not products_with_sales.empty:
            # Top sản phẩm bán chạy
            top_products = products_with_sales.sort_values('quantity_sold', ascending=False).head(5)
            
            st.markdown("### Top 5 Sản Phẩm Bán Chạy")
            
            chart_top = alt.Chart(top_products).mark_bar(color='#4CAF50').encode(
                x=alt.X('quantity_sold:Q', title='Số lượng bán', axis=alt.Axis(format=',.0f')),
                y=alt.Y('name:N', title='Tên sản phẩm', sort='-x'),
                tooltip=['name:N', 'quantity_sold:Q']
            ).properties(height=300)
            
            text_top = chart_top.mark_text(
                align='left',
                baseline='middle',
                dx=3,
                color='black'
            ).encode(
                text=alt.Text('quantity_sold:Q', format=',.0f')
            )
            
            st.altair_chart(chart_top + text_top, use_container_width=True)
            
            # Top sản phẩm bán chậm
            if len(products_with_sales) >= 5:
                slow_products = products_with_sales.sort_values('quantity_sold', ascending=True).head(5)
                
                st.markdown("### Top 5 Sản Phẩm Bán Chậm")
                
                chart_slow = alt.Chart(slow_products).mark_bar(color='#FF9800').encode(
                    x=alt.X('quantity_sold:Q', title='Số lượng bán', axis=alt.Axis(format=',.0f')),
                    y=alt.Y('name:N', title='Tên sản phẩm', sort='x'),
                    tooltip=['name:N', 'quantity_sold:Q']
                ).properties(height=300)
                
                text_slow = chart_slow.mark_text(
                    align='left',
                    baseline='middle',
                    dx=3,
                    color='black'
                ).encode(
                    text=alt.Text('quantity_sold:Q', format=',.0f')
                )
                
                st.altair_chart(chart_slow + text_slow, use_container_width=True)
            else:
                st.warning(f"Chỉ có {len(products_with_sales)} sản phẩm có doanh số bán. Cần ít nhất 5 sản phẩm để so sánh.")
        else:
            st.warning("Không có sản phẩm nào được bán trong khoảng thời gian này.")
    else:
        try:
            filtered_df['stock_remaining'] = filtered_df['stock_remaining'].astype(int)
        except Exception as e:
            st.error(f"Lỗi khi chuyển đổi stock_remaining: {str(e)}")
            st.stop()
        
        products_with_stock = filtered_df[filtered_df['stock_remaining'] > 0].copy()
        
        if not products_with_stock.empty:
            # Top sản phẩm tồn kho nhiều nhất
            top_stock_products = products_with_stock.sort_values('stock_remaining', ascending=False).head(5)
            
            st.markdown("### Top 5 Sản Phẩm Tồn Kho Nhiều Nhất")
            
            chart_top_stock = alt.Chart(top_stock_products).mark_bar(color='#2196F3').encode(
                x=alt.X('stock_remaining:Q', title='Số lượng tồn kho', axis=alt.Axis(format=',.0f')),
                y=alt.Y('name:N', title='Tên sản phẩm', sort='-x'),
                tooltip=['name:N', 'stock_remaining:Q']
            ).properties(height=300)
            
            text_top_stock = chart_top_stock.mark_text(
                align='left',
                baseline='middle',
                dx=3,
                color='black'
            ).encode(
                text=alt.Text('stock_remaining:Q', format=',.0f')
            )
            
            st.altair_chart(chart_top_stock + text_top_stock, use_container_width=True)
            
            # Top sản phẩm tồn kho ít nhất
            if len(products_with_stock) >= 5:
                slow_stock_products = products_with_stock.sort_values('stock_remaining', ascending=True).head(5)
                
                st.markdown("### Top 5 Sản Phẩm Tồn Kho Ít Nhất")
                
                chart_slow_stock = alt.Chart(slow_stock_products).mark_bar(color='#FF9800').encode(
                    x=alt.X('stock_remaining:Q', title='Số lượng tồn kho', axis=alt.Axis(format=',.0f')),
                    y=alt.Y('name:N', title='Tên sản phẩm', sort='x'),
                    tooltip=['name:N', 'stock_remaining:Q']
                ).properties(height=300)
                
                text_slow_stock = chart_slow_stock.mark_text(
                    align='left',
                    baseline='middle',
                    dx=3,
                    color='black'
                ).encode(
                    text=alt.Text('stock_remaining:Q', format=',.0f')
                )
                
                st.altair_chart(chart_slow_stock + text_slow_stock, use_container_width=True)
            else:
                st.warning(f"Chỉ có {len(products_with_stock)} sản phẩm có tồn kho. Cần ít nhất 5 sản phẩm để so sánh.")
        else:
            st.warning("Không có sản phẩm nào có tồn kho trong khoảng thời gian này.")

# ----- Biểu Đồ Theo Ngày -----
if display_mode == "Bán hàng":
    st.subheader("📈 Biểu Đồ Doanh Thu Theo Ngày")
else:
    st.subheader("📈 Biểu Đồ Tồn Kho Theo Ngày")

if not filtered_df.empty:
    daily_data = []
    for _, row in filtered_df.iterrows():
        for entry in row['stock_history']:
            try:
                entry_date = datetime.strptime(entry.get('date', ''), '%Y-%m-%d').date()
                if start_date <= entry_date <= end_date:
                    # Đảm bảo số nguyên
                    quantity_sold = max(0, int(round(float(entry.get('stock_decreased', 0)))))
                    stock_remaining = max(0, int(round(float(entry.get('stock_increased', 0)))))
                    
                    daily_data.append({
                        'date': entry_date,
                        'quantity_sold': quantity_sold,
                        'stock_remaining': stock_remaining,
                        'name': row['name'],
                        'price': row['price']
                    })
            except:
                continue
    
    if daily_data:
        daily_df = pd.DataFrame(daily_data)
        if selected_product != 'Tất cả':
            daily_df = daily_df[daily_df['name'] == selected_product]

        if not daily_df.empty:
            if display_mode == "Bán hàng":
                daily_agg = daily_df.groupby('date').agg({
                    'quantity_sold': 'sum',
                    'price': 'mean'
                }).reset_index()
                # Đảm bảo revenue là số nguyên
                daily_agg['revenue'] = (daily_agg['quantity_sold'] * daily_agg['price']).round(0).astype(int)

                # Biểu đồ doanh thu
                revenue_chart = alt.Chart(daily_agg).mark_line(point=True).encode(
                    x=alt.X('date:T', title='Ngày'),
                    y=alt.Y('revenue:Q', title='Doanh Thu (VND)', scale=alt.Scale(domain=[0, daily_agg['revenue'].max() * 1.1])),
                    tooltip=['date:T', 'revenue:Q']
                ).properties(height=300)
                
                # Biểu đồ số lượng
                quantity_chart = alt.Chart(daily_agg).mark_bar().encode(
                    x=alt.X('date:T', title='Ngày'),
                    y=alt.Y('quantity_sold:Q', title='Số Lượng Bán', scale=alt.Scale(domain=[0, daily_agg['quantity_sold'].max() * 1.1])),
                    tooltip=['date:T', 'quantity_sold:Q']
                ).properties(height=300)
                
                st.altair_chart(revenue_chart, use_container_width=True)
                st.altair_chart(quantity_chart, use_container_width=True)
            else:
                daily_stock_agg = daily_df.groupby('date').agg({
                    'stock_remaining': 'sum',
                    'price': 'mean'
                }).reset_index()
                # Đảm bảo stock_revenue là số nguyên
                daily_stock_agg['stock_revenue'] = (daily_stock_agg['stock_remaining'] * daily_stock_agg['price']).round(0).astype(int)

                # Biểu đồ doanh thu tồn kho
                stock_revenue_chart = alt.Chart(daily_stock_agg).mark_line(point=True).encode(
                    x=alt.X('date:T', title='Ngày'),
                    y=alt.Y('stock_revenue:Q', title='Doanh Thu Tồn Kho (VND)', scale=alt.Scale(domain=[0, daily_stock_agg['stock_revenue'].max() * 1.1])),
                    tooltip=['date:T', 'stock_revenue:Q']
                ).properties(height=300)
                
                # Biểu đồ số lượng tồn kho
                stock_quantity_chart = alt.Chart(daily_stock_agg).mark_bar().encode(
                    x=alt.X('date:T', title='Ngày'),
                    y=alt.Y('stock_remaining:Q', title='Số Lượng Tồn Kho', scale=alt.Scale(domain=[0, daily_stock_agg['stock_remaining'].max() * 1.1])),
                    tooltip=['date:T', 'stock_remaining:Q']
                ).properties(height=300)
                
                st.altair_chart(stock_revenue_chart, use_container_width=True)
                st.altair_chart(stock_quantity_chart, use_container_width=True)
        else:
            st.info(f"Không có dữ liệu {'doanh thu' if display_mode == 'Bán hàng' else 'tồn kho'} theo ngày để hiển thị trong khoảng thời gian này.")
    else:
        st.info(f"Không có dữ liệu {'bán hàng' if display_mode == 'Bán hàng' else 'tồn kho'} trong khoảng thời gian được chọn.")

# ----- Phân Tích Phân Khúc -----
if display_mode == "Bán hàng" and not filtered_df.empty and 'segment' in filtered_df.columns:
    st.subheader("💰 Phân Tích Phân Khúc Giá")
    
    # Tính toán phân tích phân khúc
    segment_analysis = filtered_df.groupby('segment').agg({
        'revenue': 'sum',
        'quantity_sold': 'sum'
    }).reset_index()
    
    # Tính tổng và phần trăm
    total_revenue = segment_analysis['revenue'].sum()
    total_quantity = segment_analysis['quantity_sold'].sum()
    
    segment_analysis['revenue_pct'] = (segment_analysis['revenue'] / total_revenue * 100).round(1)
    segment_analysis['quantity_pct'] = (segment_analysis['quantity_sold'] / total_quantity * 100).round(1)
    
    # Đảm bảo có đủ 3 phân khúc
    segment_order = ['Thấp', 'Trung bình', 'Cao']
    segment_colors = {
        'Thấp': '#FF6B6B',
        'Trung bình': '#4ECDC4',
        'Cao': '#45B7D1'
    }
    
    # Thêm các phân khúc thiếu
    existing_segments = set(segment_analysis['segment'].tolist())
    for segment in segment_order:
        if segment not in existing_segments:
            segment_analysis = pd.concat([segment_analysis, pd.DataFrame([{
                'segment': segment,
                'revenue': 0,
                'quantity_sold': 0,
                'revenue_pct': 0.0,
                'quantity_pct': 0.0
            }])], ignore_index=True)
    
    # Sắp xếp theo thứ tự
    segment_analysis['segment'] = pd.Categorical(
        segment_analysis['segment'],
        categories=segment_order,
        ordered=True
    )
    segment_analysis = segment_analysis.sort_values('segment')
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📈 Doanh Thu Theo Phân Khúc**")
        
        pie_revenue = alt.Chart(segment_analysis).mark_arc(
            innerRadius=50,
            outerRadius=120
        ).encode(
            theta=alt.Theta('revenue:Q'),
            color=alt.Color(
                'segment:N',
                scale=alt.Scale(
                    domain=list(segment_colors.keys()),
                    range=list(segment_colors.values())
                ),
                legend=alt.Legend(title="Phân khúc giá")
            ),
            tooltip=[
                alt.Tooltip('segment:N', title='Phân khúc'),
                alt.Tooltip('revenue:Q', title='Doanh thu', format=',.0f'),
                alt.Tooltip('revenue_pct:Q', title='Tỷ lệ (%)', format='.1f')
            ]
        ).properties(
            width=280,
            height=280,
            title=alt.TitleParams(
                text="Phân bố doanh thu",
                fontSize=14,
                anchor='start'
            )
        )
        
        st.altair_chart(pie_revenue, use_container_width=True)
        
        st.markdown(f"**💰 Tổng doanh thu: {format_number(total_revenue)} VND**")
        
        if total_revenue > 0:
            top_segment = segment_analysis.loc[segment_analysis['revenue'].idxmax()]
            st.success(f"🏆 Phân khúc **{top_segment['segment']}** dẫn đầu với **{top_segment['revenue_pct']:.1f}%** doanh thu")
    
    with col2:
        st.markdown("**📊 Số Lượng Bán Theo Phân Khúc**")
        
        pie_quantity = alt.Chart(segment_analysis).mark_arc(
            innerRadius=50,
            outerRadius=120
        ).encode(
            theta=alt.Theta('quantity_sold:Q'),
            color=alt.Color(
                'segment:N',
                scale=alt.Scale(
                    domain=list(segment_colors.keys()),
                    range=list(segment_colors.values())
                ),
                legend=alt.Legend(title="Phân khúc giá")
            ),
            tooltip=[
                alt.Tooltip('segment:N', title='Phân khúc'),
                alt.Tooltip('quantity_sold:Q', title='Số lượng', format=',.0f'),
                alt.Tooltip('quantity_pct:Q', title='Tỷ lệ (%)', format='.1f')
            ]
        ).properties(
            width=280,
            height=280,
            title=alt.TitleParams(
                text="Phân bố số lượng bán",
                fontSize=14,
                anchor='start'
            )
        )
        
        st.altair_chart(pie_quantity, use_container_width=True)
        
        st.markdown(f"**📦 Tổng số lượng bán: {format_number(total_quantity)}**")
        
        if total_quantity > 0:
            top_quantity_segment = segment_analysis.loc[segment_analysis['quantity_sold'].idxmax()]
            st.info(f"🥇 Phân khúc **{top_quantity_segment['segment']}** bán nhiều nhất với **{top_quantity_segment['quantity_pct']:.1f}%** tổng lượng")

# ----- Detailed Data Table -----
st.subheader("📋 Dữ Liệu Chi Tiết")
if not filtered_df.empty:
    st.info(f"Đang xem dữ liệu từ {len(filtered_df['source_file'].unique())} file. Thời gian hiện tại: {datetime.now().strftime('%I:%M %p +07, %d/%m/%Y')}")
    if display_mode == "Bán hàng":
        st.dataframe(
            filtered_df[['id', 'name', 'price', 'quantity_sold', 'revenue', 'segment', 'promotion', 'source_file']]
            .rename(columns={
                'id': 'ID',
                'name': 'Tên SP',
                'price': 'Giá',
                'quantity_sold': 'Số lượng bán',
                'revenue': 'Doanh thu',
                'segment': 'Phân khúc',
                'promotion': 'Khuyến mãi',
                'source_file': 'Nguồn'
            }),
            height=None,
            width=None,
            use_container_width=True
        )
    else:
        st.dataframe(
            filtered_df[['id', 'name', 'price', 'stock_remaining', 'stock_revenue', 'segment', 'promotion', 'source_file']]
            .rename(columns={
                'id': 'ID',
                'name': 'Tên SP',
                'price': 'Giá',
                'stock_remaining': 'Tồn kho',
                'stock_revenue': 'Doanh thu tồn kho',
                'segment': 'Phân khúc',
                'promotion': 'Khuyến mãi',
                'source_file': 'Nguồn'
            }),
            height=None,
            width=None,
            use_container_width=True
        )
else:
    st.warning("Không có dữ liệu để hiển thị.")

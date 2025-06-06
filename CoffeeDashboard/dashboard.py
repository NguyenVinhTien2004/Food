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

# ----- Load Data từ MongoDB được tối ưu hóa tối đa -----
@st.cache_data(ttl=config.get_cache_ttl())
def load_data_optimized():
    """Load dữ liệu được tối ưu hóa với pipeline MongoDB tiết kiệm memory"""
    # SỬA: Tạo kết nối mới mỗi lần gọi hàm thay vì sử dụng cache
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
        
        # Pipeline tối ưu - bỏ điều kiện total_sold và total_stock_increased
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
                    "price": {"$round": ["$price", 0]},
                    "promotion": 1,
                    "stock_history": {
                        "$slice": [{"$ifNull": ["$stock_history", []]}, 100]
                    }
                }
            },
            {
                "$addFields": {
                    "total_sold": {
                        "$sum": {
                            "$map": {
                                "input": "$stock_history",
                                "as": "entry",
                                "in": {"$max": [{"$ifNull": ["$$entry.stock_decreased", 0]}, 0]}
                            }
                        }
                    },
                    "total_stock_increased": {
                        "$sum": {
                            "$map": {
                                "input": "$stock_history",
                                "as": "entry", 
                                "in": {"$max": [{"$ifNull": ["$$entry.stock_increased", 0]}, 0]}
                            }
                        }
                    }
                }
            }
        ]
        
        cursor = collection.aggregate(pipeline, allowDiskUse=True, batchSize=500)
        
        all_data = []
        all_dates = set()
        
        for product in cursor:
            try:
                price = max(1000, product.get('price', 1000))
                total_sold = max(0, product.get('total_sold', 0))
                total_stock_increased = max(0, product.get('total_stock_increased', 0))
                
                stock_history = product.get('stock_history', [])[:50]
                for entry in stock_history:
                    if isinstance(entry, dict) and 'date' in entry:
                        try:
                            entry_date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                            all_dates.add(entry_date)
                        except:
                            continue
                
                all_data.append({
                    'id': str(product.get('_id', '')),
                    'name': product.get('name', ''),
                    'category': product.get('category', ''),
                    'price': price,
                    'promotion': product.get('promotion', ''),
                    'total_sold': round(total_sold, 0),
                    'revenue': round(price * total_sold, 0),
                    'total_stock_increased': round(total_stock_increased, 0),
                    'stock_revenue': round(price * total_stock_increased, 0),
                    'stock_history_str': str(stock_history),
                    'stock_history': stock_history,
                    'source_file': 'MongoDB'
                })
                
            except Exception as e:
                if config.is_debug_mode():
                    st.error(f"Error processing product: {e}")
                continue
        
        df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
        st.write(f"Số lượng dữ liệu trong df: {len(df)}")
        
        if not all_dates:
            min_date = date(2025, 3, 5)
            max_date = date(2025, 5, 25)
        else:
            min_date = min(all_dates)
            max_date = max(all_dates)
        
        # SỬA: Đóng kết nối sau khi sử dụng xong
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
    df['quantity_sold'] = df['total_sold']
    df['stock_remaining'] = df['total_stock_increased']
    
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

# ----- Hàm lọc theo ngày tối ưu -----
def filter_by_date_range_optimized(df, start_date, end_date):
    """Lọc dữ liệu theo khoảng thời gian tối ưu"""
    if df.empty:
        return df
    
    try:
        # Tạo copy để không thay đổi dataframe gốc
        filtered_df = df.copy()
        
        # Lọc stock_history theo ngày
        def filter_stock_history(stock_history):
            if not isinstance(stock_history, list):
                return []
            
            filtered_history = []
            for entry in stock_history:
                if isinstance(entry, dict) and 'date' in entry:
                    try:
                        entry_date = datetime.strptime(entry['date'], '%Y-%m-%d').date()
                        if start_date <= entry_date <= end_date:
                            filtered_history.append(entry)
                    except:
                        continue
            return filtered_history
        
        # Áp dụng lọc stock_history
        filtered_df['stock_history'] = filtered_df['stock_history'].apply(filter_stock_history)
        
        # Tính lại các metrics dựa trên stock_history đã lọc
        def recalculate_metrics(row):
            stock_history = row['stock_history']
            total_sold = 0
            total_stock_increased = 0
            
            for entry in stock_history:
                if isinstance(entry, dict):
                    total_sold += max(0, float(entry.get('stock_decreased', 0)))
                    total_stock_increased += max(0, float(entry.get('stock_increased', 0)))
            
            return pd.Series({
                'quantity_sold': total_sold,
                'stock_remaining': total_stock_increased,
                'revenue': row['price'] * total_sold,
                'stock_revenue': row['price'] * total_stock_increased
            })
        
        # Áp dụng tính toán mới
        metrics = filtered_df.apply(recalculate_metrics, axis=1)
        filtered_df['quantity_sold'] = metrics['quantity_sold']
        filtered_df['stock_remaining'] = metrics['stock_remaining']  
        filtered_df['revenue'] = metrics['revenue']
        filtered_df['stock_revenue'] = metrics['stock_revenue']
        
        return filtered_df
        
    except Exception as e:
        st.warning(f"Lỗi khi lọc dữ liệu theo ngày: {e}")
        return df

# ----- SỬA: Context manager cho MongoDB connection -----
class MongoConnection:
    """Context manager để quản lý kết nối MongoDB"""
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
    
    def __enter__(self):
        try:
            mongo_uri = config.get_mongo_uri()
            db_name = config.get_mongo_database()
            collection_name = config.get_mongo_collection()
            connection_timeout = config.get_mongo_connection_timeout()
            server_selection_timeout = config.get_mongo_server_selection_timeout()
            
            self.client = MongoClient(
                mongo_uri,
                connectTimeoutMS=connection_timeout,
                serverSelectionTimeoutMS=server_selection_timeout
            )
            
            # Test connection
            self.client.admin.command('ping')
            
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            
            return self.collection
        except Exception as e:
            st.error(f"Không thể kết nối MongoDB: {str(e)}")
            raise e
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()

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

# SỬA: Gọi hàm load dữ liệu với exception handling tốt hơn
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
    return f"{num:,.0f}".replace(",", ".")

if not filtered_df.empty:
    total_revenue = filtered_df['revenue'].sum()
    total_stock_revenue = filtered_df['stock_revenue'].sum()
    total_quantity = filtered_df['quantity_sold'].sum()
    total_stock = filtered_df['stock_remaining'].sum()
    
    # SỬA: Tính giá trung bình chỉ từ những sản phẩm có doanh thu > 0
    if display_mode == "Bán hàng":
        products_with_sales = filtered_df[filtered_df['quantity_sold'] > 0]
        avg_price = products_with_sales['price'].mean() if len(products_with_sales) > 0 else 0
    else:
        products_with_stock = filtered_df[filtered_df['stock_revenue'] > 0]
        avg_price = products_with_stock['price'].mean() if len(products_with_stock) > 0 else 0
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

# SỬA: Phần Top Sản Phẩm với logic lọc chặt chẽ hơn
st.subheader("🏆 Top Sản Phẩm")
if not filtered_df.empty:
    if display_mode == "Bán hàng":
        try:
            filtered_df['quantity_sold'] = pd.to_numeric(filtered_df['quantity_sold'], errors='coerce').fillna(0)
        except KeyError as e:
            st.error(f"Lỗi: Cột 'quantity_sold' không tồn tại trong filtered_df. Các cột hiện có: {filtered_df.columns.tolist()}")
            st.stop()
        except Exception as e:
            st.error(f"Lỗi khi chuyển đổi quantity_sold: {str(e)}")
            st.stop()
        
        # SỬA: Debug - Kiểm tra dữ liệu trước khi lọc
        if config.is_debug_mode():
            st.write("Debug - Thống kê quantity_sold:")
            st.write(f"- Tổng số sản phẩm: {len(filtered_df)}")
            st.write(f"- Sản phẩm có quantity_sold > 0: {len(filtered_df[filtered_df['quantity_sold'] > 0])}")
            st.write(f"- Giá trị quantity_sold min/max: {filtered_df['quantity_sold'].min()}/{filtered_df['quantity_sold'].max()}")
            st.write(f"- Unique values trong quantity_sold: {sorted(filtered_df['quantity_sold'].unique())}")
        
        # SỬA: Lọc sản phẩm có bán (quantity_sold > 0) - CHẶT CHẼ HƠN
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
            
            # SỬA: Logic sản phẩm bán chậm - HOÀN TOÀN MỚI
            st.markdown("### Top 5 Sản Phẩm Bán Chậm")
            
            # Kiểm tra số lượng sản phẩm có bán
            unique_products_count = len(products_with_sales)
            
            if config.is_debug_mode():
                st.write(f"Debug - Số lượng sản phẩm có bán: {unique_products_count}")
                st.write(f"Debug - Giá trị quantity_sold trong products_with_sales: {sorted(products_with_sales['quantity_sold'].unique())}")
            
            if unique_products_count >= 5:
                # SỬA: Lọc và sắp xếp cẩn thận hơn
                slow_products_candidate = products_with_sales.sort_values('quantity_sold', ascending=True)
                
                # SỬA: Đảm bảo loại bỏ tất cả sản phẩm có quantity_sold = 0
                slow_products = slow_products_candidate[slow_products_candidate['quantity_sold'] > 0].head(5)
                
                if config.is_debug_mode():
                    st.write(f"Debug - slow_products sau khi lọc:")
                    st.write(slow_products[['name', 'quantity_sold']].head())
                
                if not slow_products.empty and len(slow_products) > 0:
                    # SỬA: Kiểm tra thêm một lần nữa trước khi vẽ biểu đồ
                    slow_products_final = slow_products[slow_products['quantity_sold'] > 0]
                    
                    if len(slow_products_final) > 0:
                        chart_slow = alt.Chart(slow_products_final).mark_bar(color='#FF9800').encode(
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
                        
                        st.info("💡 **Ghi chú**: Đây là những sản phẩm có doanh số bán thấp nhất nhưng vẫn được bán ra. Cần có chiến lược marketing để cải thiện doanh số cho những sản phẩm này.")
                    else:
                        st.warning("Tất cả sản phẩm 'bán chậm' đều có quantity_sold = 0 - không thể hiển thị biểu đồ.")
                        if config.is_debug_mode():
                            st.write("Debug - Dữ liệu slow_products_final:")
                            st.write(slow_products_final[['name', 'quantity_sold']])
                else:
                    st.warning("Không có sản phẩm bán chậm phù hợp để hiển thị sau khi lọc.")
                    
            elif unique_products_count > 1:
                # Nếu có từ 2-4 sản phẩm, hiển thị tất cả sản phẩm trừ sản phẩm bán chạy nhất
                slow_products = products_with_sales.sort_values('quantity_sold', ascending=True).iloc[:-1]  # Loại bỏ sản phẩm bán chạy nhất
                
                # SỬA: Đảm bảo lọc quantity_sold > 0 ở đây cũng
                slow_products = slow_products[slow_products['quantity_sold'] > 0]
                
                if not slow_products.empty:
                    st.info(f"Hiển thị {len(slow_products)} sản phẩm bán chậm hơn (trong tổng số {unique_products_count} sản phẩm có bán).")
                    
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
                    st.warning("Không thể tạo biểu đồ sản phẩm bán chậm vì sau khi lọc quantity_sold > 0, không còn sản phẩm nào.")
                    if config.is_debug_mode():
                        st.write("Debug - Số sản phẩm trước khi lọc quantity_sold > 0:", len(products_with_sales) - 1)
            else:
                st.warning(f"Chỉ có {unique_products_count} sản phẩm có doanh số bán. Cần ít nhất 2 sản phẩm để so sánh.")
                
        else:
            st.warning("Không có sản phẩm nào được bán trong khoảng thời gian này.")
            st.info("💡 **Gợi ý**: Hãy thử mở rộng khoảng thời gian hoặc thay đổi bộ lọc danh mục/phân khúc.")
            
            # SỬA: Debug thêm thông tin về dữ liệu gốc
            if config.is_debug_mode():
                st.write("Debug - Thông tin về filtered_df:")
                st.write(f"- Tổng số dòng: {len(filtered_df)}")
                st.write(f"- Phân bố quantity_sold:")
                st.write(filtered_df['quantity_sold'].value_counts().head(10))
    
    else:
        # Logic tương tự cho chế độ tồn kho (giữ nguyên như cũ)
        try:
            filtered_df['stock_remaining'] = pd.to_numeric(filtered_df['stock_remaining'], errors='coerce').fillna(0)
        except Exception as e:
            st.error(f"Lỗi khi chuyển đổi stock_remaining: {str(e)}")
            st.stop()
        
        # SỬA: Áp dụng logic tương tự cho tồn kho
        products_with_stock = filtered_df[filtered_df['stock_remaining'] > 0].copy()
        
        if not products_with_stock.empty:
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
            
            st.markdown("### Top 5 Sản Phẩm Tồn Kho Ít Nhất")
            
            if len(products_with_stock) >= 5:
                # SỬA: Áp dụng logic lọc tương tự cho tồn kho
                slow_stock_candidates = products_with_stock.sort_values('stock_remaining', ascending=True)
                slow_stock_products = slow_stock_candidates[slow_stock_candidates['stock_remaining'] > 0].head(5)
                
                if not slow_stock_products.empty:
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
                    st.warning("Không có sản phẩm tồn kho ít phù hợp để hiển thị.")
            else:
                st.warning(f"Chỉ có {len(products_with_stock)} sản phẩm có tồn kho trong khoảng thời gian này.")
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
                    # SỬA: Đảm bảo giá trị không âm
                    quantity_sold = max(0, float(entry.get('stock_decreased', 0)))
                    stock_remaining = max(0, float(entry.get('stock_increased', 0)))
                    
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
                # SỬA: Đảm bảo revenue không âm
                daily_agg['revenue'] = (daily_agg['quantity_sold'] * daily_agg['price']).clip(lower=0).astype(int)

                # SỬA: Thiết lập trục Y bắt đầu từ 0
                revenue_chart = alt.Chart(daily_agg).mark_line(point=True).encode(
                    x=alt.X('date:T', title='Ngày'),
                    y=alt.Y('revenue:Q', title='Doanh Thu (VND)', scale=alt.Scale(domain=[0, daily_agg['revenue'].max() * 1.1])),
                    tooltip=['date:T', 'revenue:Q']
                ).properties(height=300)
                
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
                # SỬA: Đảm bảo stock_revenue không âm
                daily_stock_agg['stock_revenue'] = (daily_stock_agg['stock_remaining'] * daily_stock_agg['price']).clip(lower=0).astype(int)

                # SỬA: Thiết lập trục Y bắt đầu từ 0
                stock_revenue_chart = alt.Chart(daily_stock_agg).mark_line(point=True).encode(
                    x=alt.X('date:T', title='Ngày'),
                    y=alt.Y('stock_revenue:Q', title='Doanh Thu Tồn Kho (VND)', scale=alt.Scale(domain=[0, daily_stock_agg['stock_revenue'].max() * 1.1])),
                    tooltip=['date:T', 'stock_revenue:Q']
                ).properties(height=300)
                
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

# Sơ đồ phân khúc được tối ưu hóa
if display_mode == "Bán hàng" and not filtered_df.empty and 'segment' in filtered_df.columns:
    st.subheader("💰 Phân Tích Phân Khúc Giá")
    
    # ---- TỐI ƯU HÓA: Tính toán một lần và cache ----
    @st.cache_data
    def calculate_segment_analysis(df):
        """Tính toán phân tích phân khúc với cache để tránh tính toán lại"""
        # Nhóm và tính toán một lần
        segment_data = df.groupby('segment', observed=False).agg({
            'quantity_sold': 'sum',
            'revenue': 'sum'
        }).reset_index()
        
        # Tính tổng một lần
        total_revenue = segment_data['revenue'].sum()
        total_quantity = segment_data['quantity_sold'].sum()
        
        # Tính phần trăm
        segment_data['revenue_pct'] = (segment_data['revenue'] / total_revenue * 100).round(1)
        segment_data['quantity_pct'] = (segment_data['quantity_sold'] / total_quantity * 100).round(1)
        
        return segment_data, total_revenue, total_quantity
    
    # Gọi hàm tối ưu hóa
    segment_analysis, total_revenue, total_quantity = calculate_segment_analysis(filtered_df)
    
    # ---- XỬ LÝ PHÂN KHÚC CAO-TRUNG-THẤP ----
    # Định nghĩa thứ tự và đảm bảo đầy đủ 3 phân khúc
    segment_order = ['Cao', 'Trung', 'Thấp']
    segment_colors = {
        'Cao': '#FF6B6B',      # Đỏ cho phân khúc cao
        'Trung': '#4ECDC4',    # Xanh lá cho phân khúc trung
        'Thấp': '#45B7D1'      # Xanh dương cho phân khúc thấp
    }
    
    # Đảm bảo có đủ 3 phân khúc (thêm phân khúc thiếu với giá trị 0)
    existing_segments = set(segment_analysis['segment'].tolist())
    for segment in segment_order:
        if segment not in existing_segments:
            new_row = {
                'segment': segment, 
                'quantity_sold': 0, 
                'revenue': 0,
                'revenue_pct': 0.0,
                'quantity_pct': 0.0
            }
            segment_analysis = pd.concat([segment_analysis, pd.DataFrame([new_row])], ignore_index=True)
    
    # Sắp xếp theo thứ tự định trước
    segment_analysis['segment'] = pd.Categorical(
        segment_analysis['segment'], 
        categories=segment_order, 
        ordered=True
    )
    segment_analysis = segment_analysis.sort_values('segment').reset_index(drop=True)
    
    # ---- LOGIC PHÂN KHÚC GIÁ SỬA LẠI ----
def categorize_price_segment(df):
    """
    Phân loại sản phẩm theo phân khúc giá dựa trên percentile
    """
    if df.empty or df['price'].isna().all():
        df['segment'] = 'Không xác định'
        return df
    
    # Tính toán percentile để phân khúc
    price_25th = df['price'].quantile(0.25)
    price_75th = df['price'].quantile(0.75)
    
    # Đảm bảo có sự phân biệt giữa các phân khúc
    if price_25th == price_75th:
        # Nếu giá trị giống nhau, dùng min/max
        price_min = df['price'].min()
        price_max = df['price'].max()
        
        if price_min == price_max:
            df['segment'] = 'Trung bình'
        else:
            price_mid = (price_min + price_max) / 2
            df['segment'] = df['price'].apply(lambda x: 
                'Thấp' if x < price_mid else 'Cao'
            )
    else:
        # Phân loại theo percentile
        def classify_segment(price):
            if pd.isna(price):
                return 'Không xác định'
            elif price <= price_25th:
                return 'Thấp'
            elif price <= price_75th:
                return 'Trung bình'
            else:
                return 'Cao'
        
        df['segment'] = df['price'].apply(classify_segment)
    
    return df

# ---- ÁP DỤNG PHÂN KHÚC MỚI ----
# Giả sử filtered_df là DataFrame đã được filter
filtered_df = categorize_price_segment(filtered_df.copy())

# ---- TÍNH TOÁN PHÂN TÍCH PHÂN KHÚC ----
def calculate_segment_analysis(df, display_mode):
    """
    Tính toán phân tích theo phân khúc với logic chính xác
    """
    if df.empty:
        return pd.DataFrame(columns=['segment', 'revenue', 'quantity_sold', 'revenue_pct', 'quantity_pct'])
    
    if display_mode == "Bán hàng":
        # Tính toán cho chế độ bán hàng
        segment_stats = df.groupby('segment').agg({
            'revenue': 'sum',
            'quantity_sold': 'sum'
        }).reset_index()
        
        # Tính phần trăm
        total_revenue = segment_stats['revenue'].sum()
        total_quantity = segment_stats['quantity_sold'].sum()
        
        segment_stats['revenue_pct'] = (segment_stats['revenue'] / total_revenue * 100) if total_revenue > 0 else 0
        segment_stats['quantity_pct'] = (segment_stats['quantity_sold'] / total_quantity * 100) if total_quantity > 0 else 0
        
    else:
        # Tính toán cho chế độ tồn kho
        segment_stats = df.groupby('segment').agg({
            'stock_revenue': 'sum',
            'stock_remaining': 'sum'
        }).reset_index()
        
        # Đổi tên cột để thống nhất
        segment_stats = segment_stats.rename(columns={
            'stock_revenue': 'revenue',
            'stock_remaining': 'quantity_sold'
        })
        
        # Tính phần trăm
        total_revenue = segment_stats['revenue'].sum()
        total_quantity = segment_stats['quantity_sold'].sum()
        
        segment_stats['revenue_pct'] = (segment_stats['revenue'] / total_revenue * 100) if total_revenue > 0 else 0
        segment_stats['quantity_pct'] = (segment_stats['quantity_sold'] / total_quantity * 100) if total_quantity > 0 else 0
    
    return segment_stats

# Tính toán phân tích phân khúc
segment_analysis = calculate_segment_analysis(filtered_df, display_mode)

# ---- HIỂN THỊ BIỂU ĐỒ SỬA LẠI ----
if not segment_analysis.empty:
    # Định nghĩa thứ tự và màu sắc
    segment_order = ['Thấp', 'Trung bình', 'Cao']
    segment_colors = {
        'Thấp': '#ff6b6b',      # Đỏ nhạt
        'Trung bình': '#4ecdc4', # Xanh mint
        'Cao': '#45b7d1',       # Xanh dương
    }
    
    # Tính tổng
    total_revenue = segment_analysis['revenue'].sum()
    total_quantity = segment_analysis['quantity_sold'].sum()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**📈 Doanh Thu Theo Phân Khúc**")
        
        # Biểu đồ tròn doanh thu với tooltip chi tiết
        pie_revenue = alt.Chart(segment_analysis).mark_arc(
            innerRadius=50,
            outerRadius=120,
            stroke='white',
            strokeWidth=2
        ).encode(
            theta=alt.Theta('revenue:Q', scale=alt.Scale(type="linear")),
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
        
        # Hiển thị tổng doanh thu
        st.markdown(f"**💰 Tổng doanh thu: {total_revenue:,.0f} VND**".replace(",", "."))
        
        # Hiển thị top phân khúc
        if total_revenue > 0:
            top_segment = segment_analysis.loc[segment_analysis['revenue'].idxmax()]
            st.success(f"🏆 Phân khúc **{top_segment['segment']}** dẫn đầu với **{top_segment['revenue_pct']:.1f}%** doanh thu")
    
    with col2:
        st.markdown("**📊 Số Lượng Bán Theo Phân Khúc**")
        
        # Biểu đồ tròn số lượng
        pie_quantity = alt.Chart(segment_analysis).mark_arc(
            innerRadius=50,
            outerRadius=120,
            stroke='white',
            strokeWidth=2
        ).encode(
            theta=alt.Theta('quantity_sold:Q', scale=alt.Scale(type="linear")),
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
        
        # Hiển thị tổng số lượng
        st.markdown(f"**📦 Tổng số lượng bán: {total_quantity:,.0f}**".replace(",", "."))
        
        # Hiển thị top phân khúc theo số lượng
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

from etl_ingest import DataEngine
engine = DataEngine()
print("测试抓取中国铝业 (601600)...")
count = engine.fetch_and_save_shareholders("601600")
print(f"入库条数: {count}")

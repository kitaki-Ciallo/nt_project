# -*- coding: utf-8 -*-
import os
import re

def update_tokens():
    print("🚀 [Token管理器] 正在更新项目配置...")
    
    pushplus_token = input("请输入 PushPlus Token (直接回车跳过): ").strip()
    tushare_token = input("请输入 Tushare Pro Token (直接回车跳过): ").strip()
    
    if not pushplus_token and not tushare_token:
        print("💡 未输入任何 Token，操作已取消。")
        return

    config_path = "config.py"
    if not os.path.exists(config_path):
        print(f"❌ 错误: 未找到配置文件 {config_path}")
        return

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        new_content = content
        modified = False
        
        # 匹配 PUSHPLUS_TOKEN = "..."
        if pushplus_token:
            pattern = r'(PUSHPLUS_TOKEN\s*=\s*os\.getenv\([\'"]PUSHPLUS_TOKEN[\'"],\s*)([\'"])(.*?)([\'"])\)'
            if re.search(pattern, new_content):
                new_content = re.sub(pattern, f'\\g<1>\\g<2>{pushplus_token}\\g<4>)', new_content)
                modified = True
            else:
                # 兼容非 getenv 格式
                pattern_simple = r'(PUSHPLUS_TOKEN\s*=\s*)([\'"])(.*?)([\'"])'
                new_content = re.sub(pattern_simple, f'\\g<1>\\g<2>{pushplus_token}\\g<4>', new_content)
                modified = True
        
        # 匹配 TUSHARE_TOKEN = "..."
        if tushare_token:
            pattern = r'(TUSHARE_TOKEN\s*=\s*os\.getenv\([\'"]TUSHARE_TOKEN[\'"],\s*)([\'"])(.*?)([\'"])\)'
            if re.search(pattern, new_content):
                new_content = re.sub(pattern, f'\\g<1>\\g<2>{tushare_token}\\g<4>)', new_content)
                modified = True
            else:
                pattern_simple = r'(TUSHARE_TOKEN\s*=\s*)([\'"])(.*?)([\'"])'
                new_content = re.sub(pattern_simple, f'\\g<1>\\g<2>{tushare_token}\\g<4>', new_content)
                modified = True

        if modified:
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"✅ 成功更新 {config_path}！")
        else:
            print("⚠️ 未能在 config.py 中匹配到 Token 配置项。")
                
    except Exception as e:
        print(f"❌ 更新失败: {e}")

if __name__ == "__main__":
    update_tokens()

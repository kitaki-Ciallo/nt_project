import os
import re

def update_tokens():
    print("This script will update PushPlus and Tushare tokens in the current directory.")
    print("Scanning for .py and .sh files...")
    
    pushplus_token = input("Please enter your PushPlus Token (press Enter to skip): ").strip()
    tushare_token = input("Please enter your Tushare Pro Token (press Enter to skip): ").strip()
    
    if not pushplus_token and not tushare_token:
        print("No tokens provided. Exiting.")
        return

    # Get all files in current directory
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    
    count = 0
    for filename in files:
        if filename == "update_tokens.py":
            continue
            
        # Only process .py and .sh files
        if not (filename.endswith('.py') or filename.endswith('.sh')):
            continue
            
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            new_content = content
            modified = False
            
            # Update PushPlus Token
            if pushplus_token:
                # Python pattern: PUSHPLUS_TOKEN = "..."
                # Handles spaces around = and different quote types
                pattern_py = r'(PUSHPLUS_TOKEN\s*=\s*)([\'"])(.*?)([\'"])'
                if re.search(pattern_py, new_content):
                    # Use \g<n> to avoid ambiguity if token starts with a digit
                    safe_token = pushplus_token.replace('\\', '\\\\')
                    new_content = re.sub(pattern_py, f'\\g<1>\\g<2>{safe_token}\\g<4>', new_content)
                    modified = True
                
                # Shell pattern: PUSHPLUS_TOKEN="..."
                # Usually no spaces in shell assignment
                pattern_sh = r'(PUSHPLUS_TOKEN=)([\'"])(.*?)([\'"])'
                if re.search(pattern_sh, new_content):
                    safe_token = pushplus_token.replace('\\', '\\\\')
                    new_content = re.sub(pattern_sh, f'\\g<1>\\g<2>{safe_token}\\g<4>', new_content)
                    modified = True
            
            # Update Tushare Token
            if tushare_token:
                # Pattern: pro = ts.pro_api('...')
                pattern_ts = r'(pro\s*=\s*ts\.pro_api\s*\(\s*)([\'"])(.*?)([\'"])(\s*\))'
                if re.search(pattern_ts, new_content):
                    safe_token = tushare_token.replace('\\', '\\\\')
                    new_content = re.sub(pattern_ts, f'\\g<1>\\g<2>{safe_token}\\g<4>\\g<5>', new_content)
                    modified = True
            
            if modified:
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f"✅ Updated {filename}")
                count += 1
                
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
            
    if count == 0:
        print("No files were modified. Please check if the tokens exist in the files.")
    else:
        print(f"Successfully updated {count} files.")

if __name__ == "__main__":
    update_tokens()

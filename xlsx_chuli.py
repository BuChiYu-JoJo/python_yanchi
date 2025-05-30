import pandas as pd
import numpy as np
import re
from collections import defaultdict

# ISO 3166-1两位字母代码到中文名称的完整映射表
ISO2_TO_CHINESE = {
    'AD': '安道尔', 'AE': '阿联酋', 'AF': '阿富汗', 'AG': '安提瓜和巴布达', 'AI': '安圭拉',
    'AL': '阿尔巴尼亚', 'AM': '亚美尼亚', 'AO': '安哥拉', 'AQ': '南极洲', 'AR': '阿根廷',
    'AS': '美属萨摩亚', 'AT': '奥地利', 'AU': '澳大利亚', 'AW': '阿鲁巴', 'AX': '奥兰群岛',
    'AZ': '阿塞拜疆', 'BA': '波黑', 'BB': '巴巴多斯', 'BD': '孟加拉国', 'BE': '比利时',
    'BF': '布基纳法索', 'BG': '保加利亚', 'BH': '巴林', 'BI': '布隆迪', 'BJ': '贝宁',
    'BL': '圣巴泰勒米', 'BM': '百慕大', 'BN': '文莱', 'BO': '玻利维亚', 'BQ': '荷兰加勒比区',
    'BR': '巴西', 'BS': '巴哈马', 'BT': '不丹', 'BV': '布韦岛', 'BW': '博茨瓦纳',
    'BY': '白俄罗斯', 'BZ': '伯利兹', 'CA': '加拿大', 'CC': '科科斯群岛', 'CD': '刚果（金）',
    'CF': '中非', 'CG': '刚果（布）', 'CH': '瑞士', 'CI': '科特迪瓦', 'CK': '库克群岛',
    'CL': '智利', 'CM': '喀麦隆', 'CN': '中国', 'CO': '哥伦比亚', 'CR': '哥斯达黎加',
    'CU': '古巴', 'CV': '佛得角', 'CW': '库拉索', 'CX': '圣诞岛', 'CY': '塞浦路斯',
    'CZ': '捷克', 'DE': '德国', 'DJ': '吉布提', 'DK': '丹麦', 'DM': '多米尼克',
    'DO': '多米尼加', 'DZ': '阿尔及利亚', 'EC': '厄瓜多尔', 'EE': '爱沙尼亚', 'EG': '埃及',
    'EH': '西撒哈拉', 'ER': '厄立特里亚', 'ES': '西班牙', 'ET': '埃塞俄比亚', 'FI': '芬兰',
    'FJ': '斐济', 'FK': '马尔维纳斯群岛（福克兰）', 'FM': '密克罗尼西亚', 'FO': '法罗群岛',
    'FR': '法国', 'GA': '加蓬', 'GB': '英国', 'GD': '格林纳达', 'GE': '格鲁吉亚',
    'GF': '法属圭亚那', 'GG': '根西岛', 'GH': '加纳', 'GI': '直布罗陀', 'GL': '格陵兰',
    'GM': '冈比亚', 'GN': '几内亚', 'GP': '瓜德罗普', 'GQ': '赤道几内亚', 'GR': '希腊',
    'GS': '南乔治亚岛和南桑威奇群岛', 'GT': '危地马拉', 'GU': '关岛', 'GW': '几内亚比绍',
    'GY': '圭亚那', 'HK': '中国香港', 'HM': '赫德岛和麦克唐纳群岛', 'HN': '洪都拉斯',
    'HR': '克罗地亚', 'HT': '海地', 'HU': '匈牙利', 'ID': '印尼', 'IE': '爱尔兰',
    'IL': '以色列', 'IM': '马恩岛', 'IN': '印度', 'IO': '英属印度洋领地', 'IQ': '伊拉克',
    'IR': '伊朗', 'IS': '冰岛', 'IT': '意大利', 'JE': '泽西岛', 'JM': '牙买加',
    'JO': '约旦', 'JP': '日本', 'KE': '肯尼亚', 'KG': '吉尔吉斯斯坦', 'KH': '柬埔寨',
    'KI': '基里巴斯', 'KM': '科摩罗', 'KN': '圣基茨和尼维斯', 'KP': '朝鲜', 'KR': '韩国',
    'KW': '科威特', 'KY': '开曼群岛', 'KZ': '哈萨克斯坦', 'LA': '老挝', 'LB': '黎巴嫩',
    'LC': '圣卢西亚', 'LI': '列支敦士登', 'LK': '斯里兰卡', 'LR': '利比里亚', 'LS': '莱索托',
    'LT': '立陶宛', 'LU': '卢森堡', 'LV': '拉脱维亚', 'LY': '利比亚', 'MA': '摩洛哥',
    'MC': '摩纳哥', 'MD': '摩尔多瓦', 'ME': '黑山', 'MF': '法属圣马丁', 'MG': '马达加斯加',
    'MH': '马绍尔群岛', 'MK': '北马其顿', 'ML': '马里', 'MM': '缅甸', 'MN': '蒙古',
    'MO': '中国澳门', 'MP': '北马里亚纳群岛', 'MQ': '马提尼克', 'MR': '毛里塔尼亚',
    'MS': '蒙特塞拉特', 'MT': '马耳他', 'MU': '毛里求斯', 'MV': '马尔代夫', 'MW': '马拉维',
    'MX': '墨西哥', 'MY': '马来西亚', 'MZ': '莫桑比克', 'NA': '纳米比亚', 'NC': '新喀里多尼亚',
    'NE': '尼日尔', 'NF': '诺福克岛', 'NG': '尼日利亚', 'NI': '尼加拉瓜', 'NL': '荷兰',
    'NO': '挪威', 'NP': '尼泊尔', 'NR': '瑙鲁', 'NU': '纽埃', 'NZ': '新西兰', 'OM': '阿曼',
    'PA': '巴拿马', 'PE': '秘鲁', 'PF': '法属波利尼西亚', 'PG': '巴布亚新几内亚',
    'PH': '菲律宾', 'PK': '巴基斯坦', 'PL': '波兰', 'PM': '圣皮埃尔和密克隆群岛',
    'PN': '皮特凯恩群岛', 'PR': '波多黎各', 'PS': '巴勒斯坦', 'PT': '葡萄牙', 'PW': '帕劳',
    'PY': '巴拉圭', 'QA': '卡塔尔', 'RE': '留尼汪', 'RO': '罗马尼亚', 'RS': '塞尔维亚',
    'RU': '俄罗斯', 'RW': '卢旺达', 'SA': '沙特阿拉伯', 'SB': '所罗门群岛', 'SC': '塞舌尔',
    'SD': '苏丹', 'SE': '瑞典', 'SG': '新加坡', 'SH': '圣赫勒拿', 'SI': '斯洛文尼亚',
    'SJ': '斯瓦尔巴和扬马延', 'SK': '斯洛伐克', 'SL': '塞拉利昂', 'SM': '圣马力诺',
    'SN': '塞内加尔', 'SO': '索马里', 'SR': '苏里南', 'SS': '南苏丹', 'ST': '圣多美和普林西比',
    'SV': '萨尔瓦多', 'SX': '荷属圣马丁', 'SY': '叙利亚', 'SZ': '斯威士兰', 'TC': '特克斯和凯科斯群岛',
    'TD': '乍得', 'TF': '法属南部领地', 'TG': '多哥', 'TH': '泰国', 'TJ': '塔吉克斯坦',
    'TK': '托克劳', 'TL': '东帝汶', 'TM': '土库曼斯坦', 'TN': '突尼斯', 'TO': '汤加',
    'TR': '土耳其', 'TT': '特立尼达和多巴哥', 'TV': '图瓦卢', 'TW': '中国台湾', 'TZ': '坦桑尼亚',
    'UA': '乌克兰', 'UG': '乌干达', 'UM': '美国本土外小岛屿', 'US': '美国', 'UY': '乌拉圭',
    'UZ': '乌兹别克斯坦', 'VA': '梵蒂冈', 'VC': '圣文森特和格林纳丁斯', 'VE': '委内瑞拉',
    'VG': '英属维尔京群岛', 'VI': '美属维尔京群岛', 'VN': '越南', 'VU': '瓦努阿图',
    'WF': '瓦利斯和富图纳', 'WS': '萨摩亚', 'XK': '科索沃', 'YE': '也门', 'YT': '马约特', 'ZA': '南非',
    'ZM': '赞比亚', 'ZW': '津巴布韦'
}


def process_excel(input_path, output_path):
    error_patterns = [
        'Error: SSLError', 'HTTP_429', 'Timeout', 'Error: ChunkedEncodingError',
        'HTTP_403', 'Error: ProxyError', 'Error: ContentDecodingError', 'Error: ConnectionError'
    ]
    error_regex = r'|'.join([re.escape(p) for p in error_patterns])
    number_regex = r'^[+-]?\d+\.?\d*$'

    # 读取时保留原始空值表示
    sheets = pd.read_excel(input_path, sheet_name=None, keep_default_na=False)

    for sheet_name, df in sheets.items():
        # ========== 列结构处理 ==========
        # 插入"国家是否一致"列
        return_country_idx = df.columns.get_loc('返回国家')
        df.insert(return_country_idx + 1, '国家是否一致', '')

        # 插入延迟相关列
        delay_idx = df.columns.get_loc('延迟')
        df.insert(delay_idx + 1, '总平均延迟', np.nan)
        df.insert(delay_idx + 2, '空列', '')

        # 插入分析列
        empty_col_idx = df.columns.get_loc('空列')
        analysis_columns = [
            ('国家列表', ''),
            ('国家名称', ''),
            ('请求次数', np.nan),
            ('国家不一致次数', np.nan),
            ('平均延迟', np.nan),
            ('独立IP数', np.nan),
            ('重复率', np.nan),
            ('请求失败率', np.nan)
        ]
        for i, (col, default) in enumerate(analysis_columns):
            df.insert(empty_col_idx + 1 + i, col, default)

        # ========== 数据处理 ==========
        def process_delay(value):
            """处理延迟列，提取有效数值"""
            if pd.isna(value) or re.search(error_regex, str(value), re.IGNORECASE):
                return np.nan
            str_value = str(value).strip()
            # 修改正则表达式匹配逻辑
            match = re.search(r'([+-]?\d+\.?\d*)', str_value)  # 去掉了^和$，允许包含其他字符
            if match:
                try:
                    return float(match.group(1))
                except:
                    return np.nan
            return np.nan

        df['有效延迟'] = df['延迟'].apply(process_delay)

        # 计算国家一致性
        valid_mask = df['有效延迟'].notna()
        df.loc[valid_mask, '国家是否一致'] = np.where(
            df.loc[valid_mask, '请求国家'].astype(str).str.strip().str.upper() ==
            df.loc[valid_mask, '返回国家'].astype(str).str.strip().str.upper(),
            '是', '否'
        )
        df.loc[~valid_mask, '国家是否一致'] = 'N/A'

        # ========== 全局统计 ==========
        valid_delays = df['有效延迟'].dropna()
        total_avg = valid_delays.mean().round(2) if not valid_delays.empty else np.nan
        df.iloc[0, df.columns.get_loc('总平均延迟')] = total_avg

        # ========== 国家维度统计 ==========
        country_data = defaultdict(lambda: {
            'total': 0,
            'invalid': 0,
            'ips': set(),
            'mismatch': 0,
            'delays': []
        })

        for idx, row in df.iterrows():
            country = str(row['请求国家']).strip() if pd.notna(row['请求国家']) else ''
            data = country_data[country]
            data['total'] += 1
            if pd.isna(row['有效延迟']):
                data['invalid'] += 1
            else:
                data['ips'].add(row['IP'])
                data['delays'].append(row['有效延迟'])
                if row['国家是否一致'] == '否':
                    data['mismatch'] += 1

        # 生成国家列表
        sorted_countries = sorted(country_data.keys())
        chinese_names = [ISO2_TO_CHINESE.get(c.upper(), '未知国家') for c in sorted_countries]

        # 填充国家信息
        df['国家列表'] = pd.Series(sorted_countries + [''] * (len(df) - len(sorted_countries)))
        df['国家名称'] = pd.Series(chinese_names + [''] * (len(df) - len(sorted_countries)))

        # 计算统计指标
        for i, country in enumerate(sorted_countries):
            if i >= len(df):
                df = pd.concat([df, pd.DataFrame([{}]*(i+1-len(df)))], ignore_index=True)

            data = country_data[country]
            valid_count = data['total'] - data['invalid']

            # 计算各项指标
            avg_delay = np.mean(data['delays']).round(2) if data['delays'] else np.nan
            repeat_rate = (valid_count - len(data['ips'])) / valid_count if valid_count > 0 else 0.0
            failure_rate = data['invalid'] / data['total'] if data['total'] > 0 else 0.0

            # 强制转换为浮点数并限制范围
            df.at[i, '请求次数'] = int(data['total'])
            df.at[i, '国家不一致次数'] = int(data['mismatch'])
            df.at[i, '平均延迟'] = float(avg_delay) if not pd.isna(avg_delay) else np.nan
            df.at[i, '独立IP数'] = int(len(data['ips']))
            df.at[i, '重复率'] = float(repeat_rate)
            df.at[i, '请求失败率'] = float(failure_rate)

        df.drop('有效延迟', axis=1, inplace=True)

    # ========== Excel输出处理 ==========
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for sheet_name, df in sheets.items():
            # 列顺序整理
            base_columns = [
                '请求国家', '大洲', '返回国家', '国家是否一致',
                'IP', '延迟', '总平均延迟', '空列',
                '国家列表', '国家名称', '请求次数', '国家不一致次数',
                '平均延迟', '独立IP数', '重复率', '请求失败率'
            ]
            remaining = [col for col in df.columns if col not in base_columns]
            final_order = base_columns + remaining
            df = df[final_order]

            # 写入Excel
            df.to_excel(writer, sheet_name=sheet_name, index=False, na_rep='')

            # 获取工作簿和工作表对象
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            # 定义数字格式
            num_fmt = workbook.add_format({'num_format': '0.00'})
            percent_fmt = workbook.add_format({'num_format': '0.00%'})
            int_fmt = workbook.add_format({'num_format': '0'})

            # 设置列格式
            format_rules = {
                '总平均延迟': num_fmt,
                '平均延迟': num_fmt,
                '请求次数': int_fmt,
                '国家不一致次数': int_fmt,
                '独立IP数': int_fmt,
                '重复率': percent_fmt,
                '请求失败率': percent_fmt
            }

            for col_name, fmt in format_rules.items():
                col_idx = df.columns.get_loc(col_name)
                # 设置整列格式（包含标题）
                worksheet.set_column(col_idx, col_idx, 14, fmt)
                # 重写数据确保格式应用
                for row_idx in range(1, len(df)+1):
                    cell_value = df.iloc[row_idx-1][col_name]
                    if pd.notna(cell_value):
                        worksheet.write(row_idx, col_idx, cell_value, fmt)

            # 设置其他列的宽度
            for col_idx in range(len(df.columns)):
                if df.columns[col_idx] not in format_rules:
                    worksheet.set_column(col_idx, col_idx, 14)

if __name__ == "__main__":
    process_excel("meiguo_test.xlsx", "output_meiguo_test.xlsx")

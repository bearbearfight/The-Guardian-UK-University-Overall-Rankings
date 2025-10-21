import requests
import bson
import time
import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime

# 导入datetime模块用于获取当前系统时间

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}
url = "https://interactive.guim.co.uk/atoms/labs/2024/09/university-guide/overview/v/1755081891846/assets/data/overview.json"

# 数据库连接配置
#这里进行你的数据库配置
conn = pymysql.connect()

response = requests.get(url, headers=headers)
page_json = response.json()


def main():
    print("开始导入Guardian大学排名数据...")
    success_count = 0
    fail_count = 0

    try:
        with conn.cursor() as cursor:
            # 遍历每个机构数据
            total = len(page_json['institutions'])
            for idx, institution in enumerate(page_json['institutions'], 1):
                try:
                    # 准备数据 - 使用必要的字段，避免列名不匹配问题
                    data = {
                        'id': bson.ObjectId().__str__(),
                        'school_id': '',
                        'ranking': institution['rank2025'],
                        'school_cname': '',
                        'year_time': 2025,
                        'school_ename': institution['name'],
                        'prev_ranking': institution['rank2024'],
                        'update_date_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'guardian_score': institution['guardianScore'],
                        'satisfied_with_teaching': institution.get('percentSatisfiedWithTeaching', '') or 10000,
                        'satisfied_with_feedback': institution.get('percentSatisfiedWithAssessment', '') or 10000,
                        'student_taff_ratio': institution.get('studentStaffRatio', ''),
                        'spend_per_tudent': institution.get('expenditurePerStudent', ''),
                        'entry_tariff': institution.get('averageEntryTariff', ''),
                        'value_added_score': institution.get('valueAdded', ''),
                        'career_after_mths': institution.get('careerProspects', ''),
                        'average_teaching_score': institution.get('continuation', '') or 10000
                    }

                    # 构建插入SQL
                    columns = ', '.join(data.keys())
                    placeholders = ', '.join(['%s'] * len(data))
                    sql = f"""INSERT INTO school ({columns}) 
                           VALUES ({placeholders})"""

                    # 执行插入
                    cursor.execute(sql, list(data.values()))
                    success_count += 1

                    # 每10条数据提交一次
                    if idx % 10 == 0 or idx == total:
                        conn.commit()
                        print(f"已处理 {idx}/{total} 条数据，成功: {success_count}, 失败: {fail_count}")

                except Exception as e:
                    fail_count += 1
                    print(f"处理第 {idx} 条数据失败: {str(e)}")
                    # 跳过当前记录继续处理
                    continue

        # 最终提交
        conn.commit()
        print(f"\n数据导入完成!")
        print(f"总处理: {total} 条")
        print(f"成功导入: {success_count} 条")
        print(f"导入失败: {fail_count} 条")

    except Exception as e:
        print(f"数据库操作失败: {str(e)}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\n程序运行时间: {end_time - start_time:.2f} 秒")


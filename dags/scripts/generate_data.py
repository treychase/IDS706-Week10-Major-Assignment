"""
Generate realistic Super Mario Maker sample data

Creates courses.csv and records.csv in the data/ directory
with authentic gaming metrics and relationships.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path

# Set random seed
np.random.seed(42)
random.seed(42)

# Configuration
NUM_COURSES = 150
OUTPUT_DIR = Path(__file__).parent.parent  # data/ directory

def generate_course_names():
    """Generate creative Super Mario Maker course names"""
    prefixes = [
        'Precision', 'Speed', 'Kaizo', 'Auto', 'Music', 'Puzzle', 'Castle',
        'Underwater', 'Sky', 'Desert', 'Ghost', 'Rainbow', 'Fire', 'Ice',
        'Coin', 'Shell', 'Thwomp', 'Goomba', 'Koopa', 'Bowser', 'Yoshi'
    ]
    
    themes = [
        'Challenge', 'Run', 'Escape', 'Paradise', 'Nightmare', 'Adventure',
        'Trial', 'Course', 'Level', 'World', 'Zone', 'Stage', 'Path',
        'Fortress', 'Tower', 'Cave', 'Sky', 'Land'
    ]
    
    names = []
    for _ in range(NUM_COURSES):
        if random.random() < 0.7:
            name = f"{random.choice(prefixes)} {random.choice(themes)}"
        else:
            name = f"{random.choice(themes)} {random.randint(1, 10)}"
        
        if random.random() < 0.3:
            name += f" v{random.randint(1, 5)}"
        
        names.append(name)
    
    return names


def generate_courses():
    """Generate Super Mario Maker courses data"""
    
    course_names = generate_course_names()
    course_ids = [f"SMM-{i:04d}-{random.randint(1000, 9999)}" for i in range(NUM_COURSES)]
    
    # Creator IDs
    num_creators = int(NUM_COURSES * 0.6)
    creator_pool = [f"PLAYER-{i:05d}" for i in range(num_creators)]
    creator_ids = random.choices(creator_pool, k=NUM_COURSES)
    
    # Game styles
    game_styles = ['SMB1', 'SMB3', 'SMW', 'NSMBU']
    styles = random.choices(game_styles, k=NUM_COURSES, weights=[0.3, 0.25, 0.25, 0.2])
    
    # Themes
    themes = ['Ground', 'Underground', 'Underwater', 'Ghost House', 'Airship', 
              'Castle', 'Snow', 'Desert']
    course_themes = random.choices(themes, k=NUM_COURSES)
    
    # Difficulty levels
    difficulties = ['Easy', 'Normal', 'Expert', 'Super Expert']
    diff_weights = [0.15, 0.35, 0.35, 0.15]
    course_difficulties = random.choices(difficulties, k=NUM_COURSES, weights=diff_weights)
    
    # Clear rates (inversely correlated with difficulty)
    clear_rates = []
    for diff in course_difficulties:
        if diff == 'Easy':
            rate = np.random.uniform(40, 80)
        elif diff == 'Normal':
            rate = np.random.uniform(15, 45)
        elif diff == 'Expert':
            rate = np.random.uniform(3, 18)
        else:  # Super Expert
            rate = np.random.uniform(0.1, 5)
        clear_rates.append(round(rate, 2))
    
    # Play counts
    base_plays = np.random.lognormal(mean=5, sigma=2, size=NUM_COURSES)
    play_counts = [max(10, int(p)) for p in base_plays]
    
    # Clear and like counts
    clear_counts = [int(plays * (rate / 100)) for plays, rate in zip(play_counts, clear_rates)]
    like_counts = [max(0, int(plays * random.uniform(0.05, 0.25))) for plays in play_counts]
    
    # Upload dates
    start_date = datetime(2024, 1, 1)
    upload_dates = [
        (start_date + timedelta(days=random.randint(0, 300))).strftime('%Y-%m-%d')
        for _ in range(NUM_COURSES)
    ]
    
    # Create DataFrame
    courses_df = pd.DataFrame({
        'course_id': course_ids,
        'course_name': course_names,
        'creator_id': creator_ids,
        'game_style': styles,
        'theme': course_themes,
        'difficulty': course_difficulties,
        'clear_rate': clear_rates,
        'play_count': play_counts,
        'clear_count': clear_counts,
        'like_count': like_counts,
        'upload_date': upload_dates
    })
    
    # Add some missing values (3%)
    missing_indices = np.random.choice(NUM_COURSES, size=int(NUM_COURSES * 0.03), replace=False)
    courses_df.loc[missing_indices, 'clear_rate'] = np.nan
    
    return courses_df


def generate_records(courses_df):
    """Generate player completion records"""
    
    records = []
    record_id = 1
    
    for _, course in courses_df.iterrows():
        course_id = course['course_id']
        difficulty = course['difficulty']
        
        # Number of records per course
        num_records = random.randint(2, 15)
        
        # Generate player IDs
        players = [f"PLAYER-{random.randint(1, 50000):05d}" for _ in range(num_records)]
        
        # Clear times based on difficulty
        if difficulty == 'Easy':
            base_time = np.random.uniform(20, 90)
        elif difficulty == 'Normal':
            base_time = np.random.uniform(40, 180)
        elif difficulty == 'Expert':
            base_time = np.random.uniform(60, 300)
        else:  # Super Expert
            base_time = np.random.uniform(120, 600)
        
        for player in players:
            clear_time = max(10, base_time + np.random.normal(0, base_time * 0.2))
            
            days_offset = random.randint(0, 90)
            record_date = (datetime.now() - timedelta(days=days_offset)).strftime('%Y-%m-%d %H:%M:%S')
            
            attempts = random.randint(1, 50)
            
            records.append({
                'record_id': f"REC-{record_id:07d}",
                'course_id': course_id,
                'player_id': player,
                'clear_time': round(clear_time, 2),
                'attempts': attempts,
                'recorded_at': record_date
            })
            
            record_id += 1
    
    records_df = pd.DataFrame(records)
    
    # Add some missing values (2%)
    missing_indices = np.random.choice(len(records_df), 
                                      size=int(len(records_df) * 0.02), 
                                      replace=False)
    records_df.loc[missing_indices, 'clear_time'] = np.nan
    
    return records_df


def main():
    """Generate and save sample data"""
    
    print("=" * 70)
    print("  Super Mario Maker Sample Data Generator")
    print("=" * 70)
    print()
    
    # Generate courses
    print(f"📁 Generating {NUM_COURSES} courses...")
    courses_df = generate_courses()
    
    # Generate records
    print("🏃 Generating player records...")
    records_df = generate_records(courses_df)
    
    # Save files
    courses_path = OUTPUT_DIR / 'courses.csv'
    records_path = OUTPUT_DIR / 'records.csv'
    
    courses_df.to_csv(courses_path, index=False)
    records_df.to_csv(records_path, index=False)
    
    print()
    print("✅ Data generation complete!")
    print()
    print(f"📊 Created files:")
    print(f"  - {courses_path} ({len(courses_df)} courses)")
    print(f"  - {records_path} ({len(records_df)} records)")
    print()
    print("📈 Statistics:")
    print(f"  Total Courses: {len(courses_df)}")
    print(f"  Total Records: {len(records_df)}")
    print(f"  Unique Players: {records_df['player_id'].nunique()}")
    print(f"  Avg Clear Rate: {courses_df['clear_rate'].mean():.2f}%")
    print(f"  Total Plays: {courses_df['play_count'].sum():,}")
    print(f"  Total Likes: {courses_df['like_count'].sum():,}")
    print()
    print("🎮 Difficulty Distribution:")
    for diff, count in courses_df['difficulty'].value_counts().items():
        print(f"  - {diff}: {count} ({count/len(courses_df)*100:.1f}%)")
    print()
    print("=" * 70)
    print("Ready to use with Airflow pipeline!")
    print("Run: docker-compose up -d")
    print("=" * 70)


if __name__ == '__main__':
    main()
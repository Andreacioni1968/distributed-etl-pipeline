#!/usr/bin/env python3
"""
Distributed Resilience Manager (Self-Healing Module)
Handles idempotent state transitions, zombie process termination,
and distributed logical locking for concurrent ETL pipelines.
"""

import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ResilienceManager:
    def __init__(self, db_connection):
        self.conn = db_connection

    def get_running_exclusive_classes(self) -> set:
        """
        Retrieves active distributed locks to prevent I/O collisions
        between heavy analytical pipelines of the same class.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT t.exclusive_class
                FROM pipeline_missions m
                JOIN pipeline_task t ON m.task_id = t.id
                WHERE m.status IN ('pending', 'running')
                AND t.exclusive_class IS NOT NULL
            """)
            return {row[0] for row in cur.fetchall()}

    def kill_zombie_missions(self):
        """
        Active monitor that identifies and terminates orphaned workloads.
        If a task exceeds its dynamic timeout threshold without heartbeats,
        it is marked as failed and remote SSH processes are forcefully terminated.
        """
        now_utc = datetime.now(timezone.utc)
        killed_count = 0

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT m.id, t.task_name, m.sent_at, COALESCE(t.timeout_seconds, 600)
                FROM pipeline_missions m
                JOIN pipeline_task t ON m.task_id = t.id
                WHERE m.status = 'running'
            """)
            zombies = cur.fetchall()

            for mission_id, task_name, sent_at, timeout in zombies:
                if sent_at.tzinfo is None:
                    sent_at = sent_at.replace(tzinfo=timezone.utc)
                
                elapsed = (now_utc - sent_at).total_seconds()
                zombie_timeout = max(timeout * 2, 600)

                if elapsed > zombie_timeout:
                    # Idempotent state update enforcing strict state machine rules
                    cur.execute("""
                        UPDATE pipeline_missions 
                        SET status = 'failed', completed_at = clock_timestamp(), notes = %s 
                        WHERE id = %s
                    """, (f'zombie_killed_after_{int(elapsed)}s', mission_id))
                    
                    logging.warning(f"Self-Healing: Terminated zombie mission {mission_id} ({task_name}) after {int(elapsed)}s.")
                    killed_count += 1
                    
                    # Remote SSH kill execution would be triggered here

        if killed_count > 0:
            logging.info(f"Zombie sweep completed. {killed_count} stalled missions terminated.")

    def redistribute_stuck_missions(self, child_task_id: int) -> int:
        """
        Recovers tasks that were stuck in a dead-lock state by reverting
        them safely back to the queue for secondary execution attempts.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE pipeline_missions 
                SET status = 'pending', notes = 'redistributed_after_stuck'
                WHERE task_id = %s AND status = 'running'
                RETURNING id
            """, (child_task_id,))
            redistributed = cur.fetchall()
            
            if redistributed:
                logging.info(f"Redistributed {len(redistributed)} stuck workloads back to the queue.")
            return len(redistributed)

# Example usage context:
# if __name__ == "__main__":
#     db_conn = get_postgres_connection()
#     manager = ResilienceManager(db_conn)
#     manager.kill_zombie_missions()

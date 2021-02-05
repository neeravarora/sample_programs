
class Partial_HQL_Const:

    ultimateowner_id_list_filteration_join = '''
                INNER JOIN (
                    SELECT distinct trim(ultimateowner_id) as ultimateowner_id FROM (SELECT array({ultmateowner_id_list_str}) as ultimateowner_id_list ) uoids
                    LATERAL VIEW explode(uoids.ultimateowner_id_list) myTable AS ultimateowner_id
                ) t11
				ON t4.ultimateowner_id = t11.ultimateowner_id
    '''

    ultimateowner_id_filteration_join = '''
                INNER JOIN (
					SELECT trim(ultimateowner_id) AS ultimateowner_id
					FROM {db}.{hosted_ultimateowner_id}
					GROUP BY
						trim(ultimateowner_id) 
				) t11
				ON t4.ultimateowner_id = t11.ultimateowner_id
    '''
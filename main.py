import happybase
import pandas as pd

# Bloque principal de ejecución
try:
# 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")
# 2. Crear la tabla con las familias de columnas
    table_name = 'occupation'
    families = {'personal_info': dict(),  # información personal
                'job_info': dict(),  # trabajo y el entorno laboral
                'career_info': dict(),  # desarrollo profesional
                'technology_info': dict()}  # adopción de tecnología

# Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)
# Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'occupation' creada exitosamente")
# 3. Cargar datos del CSV
    occupation_data = pd.read_csv('career_change_prediction_dataset.csv')

# Iterar sobre el DataFrame usando el índice
    for index, row in occupation_data.iterrows():
# Generar row key basado en el índice
        row_key = f'occupation_{index}'.encode()
# Organizar los datos en familias de columnas
        data = {b'personal_info:Field of Study': str(row['Field of Study']).encode(),
                b'personal_info:Current Occupation': str(row['Current Occupation']).encode(),
                b'personal_info:Age': str(row['Age']).encode(),
                b'personal_info:Gender': str(row['Gender']).encode(),
                b'personal_info:Education Level': str(row['Education Level']).encode(),

                b'career_info:Career Change Interest': str(row['Career Change Interest']).encode(),
                b'career_info:Skills Gap': str(row['Skills Gap']).encode(),
                b'career_info:Family Influence': str(row['Family Influence']).encode(),
                b'career_info:Mentorship Available': str(row['Mentorship Available']).encode(),
                b'career_info:Certifications': str(row['Certifications']).encode(),
                b'career_info:Freelancing Experience': str(row['Freelancing Experience']).encode(),
                b'career_info:Career Change Events': str(row['Career Change Events']).encode(),

                b'job_info:Years of Experience': str(row['Years of Experience']).encode(),
                b'job_info:Industry Growth Rate': str(row['Industry Growth Rate']).encode(),
                b'job_info:Job Satisfaction': str(row['Job Satisfaction']).encode(),
                b'job_info:Work-Life Balance': str(row['Work-Life Balance']).encode(),
                b'job_info:Job Opportunities': str(row['Job Opportunities']).encode(),
                b'job_info:Salary': str(row['Salary']).encode(),
                b'job_info:Job Security': str(row['Job Security']).encode(),
                b'job_info:Geographic Mobility': str(row['Geographic Mobility']).encode(),
                b'job_info:Likely to Change Occupation': str(row['Likely to Change Occupation']).encode(),

                b'technology_info:Professional Networks': str(row['Professional Networks']).encode(),
                b'technology_info:Technology Adoption': str(row['Technology Adoption']).encode()}
        table.put(row_key, data)

    print("Datos cargados exitosamente")
# 4. Consultas y Análisis de Datos
    print("\n=== Todos los occupation en la base de datos (primeros 3) ===")
    count = 0
    for key, data in table.scan():
        if count < 3:  # Limitamos a 3 para el ejemplo
            print(f"\noccupation ID: {key.decode()}")
            print(f"Field of Study: {data[b'personal_info:Field of Study'].decode()}")
            print(f"Current Occupation: {data[b'personal_info:Current Occupation'].decode()}")
            print(f"Salary: {data[b'job_info:Salary'].decode()}")
        count += 1

# 6. Encontrar Ocupaciones con Salarios mas Altos
    print("\n=== Ocupaciones con salarios menores a 30016 ===")
    for key, data in table.scan():
        if int(data[b'job_info:Salary'].decode()) > 30029:
            print(f"\nOcupacion ID: {key.decode()}")
            print(f"Ocupacion: {data[b'personal_info:Current Occupation'].decode()}")
            print(f"Salario: {data[b'job_info:Salary'].decode()}")


# 7. Cantidad de Ocupaciones registradas
    print("\n=== Cantidad de registros de Ocupaciones ===")
    ocupacion = {}
    for key, data in table.scan():
        ocupa = data[b'personal_info:Current Occupation'].decode()
        ocupacion[ocupa] = ocupacion.get(ocupa, 0) + 1

    for ocupa, count in ocupacion.items():
        print(f"{ocupa}: {count} registros")

# 8. Promedio de salarios por ocupacion
    print("\n=== Salario promedio por ocupacion ===")
    ocupacion_salario = {}
    ocupacion_counts = {}
    for key, data in table.scan():
        ocupacion = data[b'personal_info:Current Occupation'].decode()
        salario = int(data[b'job_info:Salary'].decode())

        ocupacion_salario[ocupacion] = ocupacion_salario.get(ocupacion, 0) + salario
        ocupacion_counts[ocupacion] = ocupacion_counts.get(ocupacion, 0) + 1

    for ocupacion in ocupacion_salario:
        avg_salario = ocupacion_salario[ocupacion] / ocupacion_counts[ocupacion]
        print(f"{ocupacion}: {avg_salario:.2f}")

# 9. Top 5 registros de campo de estudio con mayor salario
    print("\n=== Top 5 resgistros de campo de estudio con mayores salarios ===")
    field_by_salario = []
    for key, data in table.scan():
        field_by_salario.append({'id': key.decode(),
                                 'Campo de estudio': data[b'personal_info:Field of Study'].decode(),
                                 'Education Level': str(data[b'personal_info:Education Level'].decode()),
                                 'Salario': int(data[b'job_info:Salary'].decode())})

    for field in sorted(field_by_salario, key=lambda x: x['Salario'], reverse=True)[:5]:
        print(f"ID: {field['id']}")
        print(f"Campo de estudio: {field['Campo de estudio']}")
        print(f"Education Level: {field['Education Level']}")
        print(f"Salario: {field['Salario']}\n")

# 10. Insertar registro
    index = index + 1
    row_key = f'occupation_{index}'.encode()
    new_data = {
            # Familia 'personal_info'
            b'personal_info:Field of Study': b'Biology',
            b'personal_info:Current Occupation': b'Business Analyst',
            b'personal_info:Age': b'140',
            b'personal_info:Gender': b'Female',
            b'personal_info:Education Level': b'PhD',

            # Familia 'career_info'
            b'career_info:Career Change Interest': b'0',  # 0 indica que no está interesada en cambiar de carrera
            b'career_info:Skills Gap': b'4',  # Brecha de habilidades (1-10)
            b'career_info:Family Influence': b'High',  # Alta influencia familiar
            b'career_info:Mentorship Available': b'1',  # Tiene mentor disponible
            b'career_info:Certifications': b'2',  # Tiene dos certificaciones
            b'career_info:Freelancing Experience': b'1',  # Tiene experiencia como freelancer
            b'career_info:Career Change Events': b'0',  # No ha tenido eventos de cambio de carrera

            # Familia 'job_info'
            b'job_info:Years of Experience': b'60',  # 60 años de experiencia
            b'job_info:Industry Growth Rate': b'High',  # Alta tasa de crecimiento industrial
            b'job_info:Job Satisfaction': b'9',  # Alta satisfacción laboral
            b'job_info:Work-Life Balance': b'8',  # Buen equilibrio entre vida personal y trabajo
            b'job_info:Job Opportunities': b'70',  # Oportunidades de trabajo
            b'job_info:Salary': b'200000',  # Salario en dólares
            b'job_info:Job Security': b'9',  # Alta seguridad en el trabajo
            b'job_info:Geographic Mobility': b'0',  # No está dispuesta a mudarse
            b'job_info:Likely to Change Occupation': b'0',  # No cambiará de ocupación

            # Familia 'technology_info'
            b'technology_info:Professional Networks': b'8',  # Redes profesionales (1-10)
            b'technology_info:Technology Adoption': b'7'}  # Adopción de tecnología (1-10)

    table.put(row_key, new_data)
    print(f"Nuevo registro insertado con row key {row_key.decode()}")

    record = table.row(row_key)

    # 11. Mostrar todos los campos del registro
    if record:
        print(f"Registro para row_key: {row_key.decode()}")
        for column, value in record.items():
            print(f"{column.decode()} : {value.decode()}")
    #12. Actualizar Registro

    row_key = b'occupation_9998'  # Asegúrate de que este row_key existe

    updated_data = {b'job_info:Salary': b'500000'}  # Nuevo salario
    table.put(row_key, updated_data)
    print(f"Registro con row_key '{row_key.decode()}' actualizado exitosamente.")
    record = table.row(row_key)

    #11. Mostrar todos los campos del registro actualizado
    if record:
        print(f"Registro para row_key: {row_key.decode()}")
        for column, value in record.items():
            print(f"{column.decode()} : {value.decode()}")

    row_key ='occupation_9958'
    # 4. Eliminar el registro usando delete()
    table.delete(row_key)
    print(f"Registro con row_key '{row_key.decode()}' eliminado exitosamente.")

    # 5. Verificar si el registro fue eliminado (esto debería devolver un diccionario vacío si se eliminó)
    record = table.row(row_key)
    if not record:
        print(f"El registro con row_key '{row_key}' ha sido eliminado.")
    else:
        print(f"El registro con row_key '{row_key}' sigue existiendo.")


except Exception as e:
    print(f"Error: {str(e)}")
finally:
# Cerrar la conexión
    connection.close()

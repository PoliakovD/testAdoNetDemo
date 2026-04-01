DELIMITER $$

CREATE PROCEDURE update_product_price(
    IN p_id INT,
    IN p_new_price DECIMAL(10, 2),
    OUT p_old_price DECIMAL(10, 2)
)
READS SQL DATA
DETERMINISTIC
BEGIN
    -- Получаем старую цену
    SELECT price INTO p_old_price 
    FROM table_products 
    WHERE id = p_id;

    -- Обновляем цену товара
    UPDATE table_products 
    SET price = p_new_price 
    WHERE id = p_id;
END$$

DELIMITER ;